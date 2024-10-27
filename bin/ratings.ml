open Base
open Stdio

let first_last_financials = Stocks_db.select_first_and_last_fcf ()
let eps_growth_list = Stocks_db.select_eps_growth ()

let calc_eq_discount ?(expected_return = 0.10) pe market_cap debt =
  let ep_disc = (408.0 +. pe) /. 420.0 in
  let eq_disc = market_cap /. (market_cap +. debt) *. expected_return in
  ep_disc *. eq_disc

let calc_debt_discount market_cap debt tax bond_rate =
  debt /. market_cap *. bond_rate *. (1.0 -. tax)

let calc_growth new_fcf old_fcf duration =
  let old_fcf = if Float.(>) old_fcf 0.0 then old_fcf else new_fcf /. 1.5 in 
  let increase = (new_fcf -. old_fcf) /. Float.abs old_fcf +. 1.0 in
  let growth = increase **. (1.0 /. (duration -. 1.0)) -. 1.0 in
  if Float.(<=) growth 0.0 then 0.005 else growth 
  
let get_industry_rating industry sector =
  let industry_risk, sector_risk =
    Stocks_db.select_sector_and_industry sector industry
  in
  let industry_and_sector_risk =
    (industry_risk +. sector_risk) /. 20.0 +. 0.95
  in
  industry_and_sector_risk 

let calc_discount market_cap pe debt tax bond_rate
    industry_rating =
  let eq_disc = calc_eq_discount pe market_cap debt in
  let debt_disc = calc_debt_discount market_cap debt tax bond_rate in
  (eq_disc +. debt_disc) *. industry_rating

let calc_DFCA cash_flow growth discount =
  let rec loop cash_flow year limiter growth growth_multiplier = 
    (match year with
    | 10.0 -> (0.0, 0.0)
    | year ->
      let current_year_growth = growth *. (if Float.(<) limiter 0.0 then growth_multiplier else 1.0) in
      let cash_flow_growth =
        cash_flow *. (1.0 +. current_year_growth) 
      in
      let cash_flow_discounted =
        cash_flow_growth /. ((1.0 +. discount) **. year)
      in
      let new_limiter = limiter -. Float.abs growth in
      let x, y = loop cash_flow_growth (year +. 1.0) new_limiter current_year_growth growth_multiplier in
      (cash_flow_growth +. x, cash_flow_discounted +. y)) 
  in
  let multiplier = 0.95 -. Float.abs(growth) **. 0.9 /. 5.0 in
  loop cash_flow 0.0 0.8 growth multiplier

let calc_peter_lynch_value eps_growth pe div_yield =
  (eps_growth +. div_yield) *. 100.0 /. pe

let calc_terminal_value pe growth_of_10y discount =
  let term_val = pe *. growth_of_10y in
  (term_val, term_val /. ((1.0 +. discount) **. 10.0))

let calc_intrinsic_value pe cash_flow growth discount =
  let growth_10y, disc_10y = calc_DFCA cash_flow growth discount in
  let _, disc_term_val = calc_terminal_value pe growth_10y discount in
  disc_term_val +. disc_10y

let calc_upside market_cap ttm instrinsic_value =
  (instrinsic_value /. (market_cap *. ttm)) -. 1.0

let get_intrinsic_price price upside pl_value = 
  let peter_l_ratio = 0.2 *. (pl_value /. 1.5) in
  let dcf_ratio = 0.8 *. (1.0 +. upside) in
  let full_ratio = dcf_ratio +. peter_l_ratio in
  let a = price *. full_ratio in
  a

let rate_stock_price intrinsic_price current_price target =
  let rating = current_price /. intrinsic_price in
  let target_rating = rating /. target in
  (rating, target_rating)

let print_price_rating ratings =
  let max_col = 5 in
  let row_len = 34 in  
  let rec seperate_row ?(max = 0) col =
    let max = if max < col then col else max in
    if col >= 1 then (
      if col = max then printf "+";
      String.make (row_len - 2) '-' |> printf "%s+";
      seperate_row ~max:max (col - 1))
    else print_endline ""
  in
  if max_col <= (List.length ratings) then
    seperate_row max_col
  else
    seperate_row (List.length ratings);
  let rec print ratings max_col col =
    match ratings with
    | hd :: tl ->
        let ticker_symbol, discount, price = hd in
        printf "| %*s" (-6) ticker_symbol;
        if Float.(<) price 10.0 then
          printf "C: %*.3f€ " (5) (price)
        else if Float.(<) price 1000.0 then 
          printf "C: %*.1f€ " (5) (price)
        else 
          printf "C: %*.0f€ " (5) (price);
        printf "T:";
        printf "%*.0f%% " (4) (discount *. 100.0);
        if Float.(<) (price/.discount) 10.0 then
          printf "%*.3f€ " (5) (price /. discount)
        else if Float.(<) (price/.discount) 1000.0 then
          printf "%*.1f€ " (5) (price /. discount)
        else
          printf "%*.0f€ " (5) (price /. discount);
        if col = 1 then (
          print_endline "|";
          seperate_row max_col;
          print tl max_col max_col)
        else print tl max_col (col - 1)
    | [] ->
        if max_col - col > 0 then
        (print_endline "|";
        max_col - col |> seperate_row)
  in
  print ratings max_col max_col

let filter_by_status stock_status filter =
    match filter with
          | "none" -> true
          | h -> String.(=) h (String.lowercase stock_status) 

let filter_under price_discount lower_treshold treshold =
          if
            Float.( <= ) price_discount treshold
            && Float.( > ) price_discount lower_treshold
          then true
          else false
        
let rate_stocks ?(filter = "none") stock_data =
  let rec ratings filter stock_data =
    match stock_data with
    | [] -> []
    | hd :: tl ->
        let ( tick_symbol,
              industry,
              sector,
              market_cap,
              pe,
              price,
              debt,
              tax,
              bond_rate,
              status,
              target,
              div_yield ) =
          hd
        in
        (* TODO check if desired time gap exists *) 
        let filtered_fl_financials =
          List.filter first_last_financials ~f:(
          fun (a, _, _, _) -> String.(=) a tick_symbol) 
        in
        let max_period = 
          List.map filtered_fl_financials ~f:(fun (_, _, _, d) -> d) 
          |> List.fold ~init:Float.min_value ~f:Float.max
        in
        let _, new_fcf, old_fcf, duration = List.find_exn filtered_fl_financials~f:(
          fun (_, _, _, n) -> Float.(=) n max_period)
        in
          
        let industry_rating = get_industry_rating industry sector in
        let discount =
          calc_discount market_cap pe debt tax bond_rate industry_rating 
        in
        let growth = calc_growth new_fcf old_fcf duration in
        let intrinsic_value =
          calc_intrinsic_value pe new_fcf growth discount
        in
        let upside = calc_upside market_cap pe intrinsic_value in
        
        let filtered_eps = 
          List.filter eps_growth_list ~f:(
          fun (a, _, _) -> String.(=) a tick_symbol) 
        in
        let max_period_eps =  
          List.map filtered_eps ~f:(fun (_, _, d) -> d) 
          |> List.fold ~init:Float.min_value ~f:Float.max
        in
        let _, eps_growth, _ = List.find_exn filtered_eps ~f:(
          fun (_, _, n) ->  Float.(=) n max_period_eps)
        in
        let pl_value = calc_peter_lynch_value eps_growth pe div_yield in
        let intrinsic_price = get_intrinsic_price price upside pl_value in
        let rating, target_rating = rate_stock_price intrinsic_price price target in
       
        Stocks_db.insert_ratings tick_symbol rating target_rating;
        let is_printable =
        match filter with
        | "cheap" -> filter_under target_rating 0.0 0.6
        | "low" -> filter_under target_rating 0.6 0.8
        | "under" -> filter_under target_rating 0.8 1.0
        | "fair" -> filter_under target_rating 1.0 1.3
        | hd -> filter_by_status status hd 
        in
        if is_printable then
          [ (tick_symbol, target_rating, price) ] @ ratings filter tl
        else ratings filter tl
  in
  ratings filter stock_data |> print_price_rating

let stock_ratings filter =
  let data = Stocks_db.select_ratings_data () in
  rate_stocks ~filter data
