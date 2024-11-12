open Base
open Stdio

let calc_eq_discount ?(expected_return = 0.20) market_cap debt =
  let eq_disc = market_cap /. (market_cap +. debt) *. expected_return in
  eq_disc

let calc_debt_discount market_cap debt tax bond_rate =
  debt /. market_cap *. bond_rate *. (1.0 -. tax)

let calc_growth new_fcf old_fcf duration =
  let old_fcf = if Float.(>) old_fcf 0.0 then old_fcf else new_fcf /. 1.5 in 
  let increase = (new_fcf -. old_fcf) /. Float.abs old_fcf +. 1.0 in
  let growth = increase **. (1.0 /. (duration -. 1.0)) -. 1.0 in
  if Float.(<=) growth 0.0 || Float.is_nan growth then 0.0005 else growth 
  
let calc_industry_rating industry_risk sector_risk =
  let industry_and_sector_risk =
    (industry_risk *. 0.6 +. sector_risk *. 0.4) 
  in
  industry_and_sector_risk 

let get_industry_rating industry sector =
  let industry_risk, sector_risk =
    Stocks_db.select_sector_and_industry sector industry
  in
  calc_industry_rating industry_risk sector_risk

let calc_discount market_cap debt tax bond_rate
    industry_rating =
  let eq_disc = calc_eq_discount market_cap debt in
  let debt_disc = calc_debt_discount market_cap debt tax bond_rate in
  (eq_disc +. debt_disc) *. industry_rating

let calc_DFCA cash_flow growth discount =
  let rec loop cash_flow year limiter growth growth_multiplier = 
    (match year with
    | 10.0 -> []
    | year ->
      let current_year_growth = growth *. (if Float.(<) limiter 0.0 then growth_multiplier else 1.0) in
      let cash_flow_growth =
        cash_flow *. (1.0 +. current_year_growth) 
      in
      let cash_flow_discounted =
        cash_flow_growth /. ((1.0 +. discount) **. year)
      in
      let new_limiter = limiter -. Float.abs growth in
      (cash_flow_growth, cash_flow_discounted)
       :: loop cash_flow_growth (year +. 1.0) new_limiter current_year_growth growth_multiplier 
    ) 
  in
  let limit = 0.2 in
  let growth_limited = if Float.(<) growth limit then growth else limit in
  let multiplier = 0.9 -. Float.abs(growth_limited) /. 3.0 in
  let dcf_list = loop cash_flow 0.0 0.2 growth_limited multiplier |> List.rev in
  let growth_10y = 
    match dcf_list with
    | (hd, _) :: _ -> hd
    | _ -> 0.0
  in
  let discount_sum = List.fold ~init:0.0 dcf_list ~f:(fun acc (_, a) -> acc +. a) in
  (growth_10y, discount_sum)  

let calc_peter_lynch_value eps_growth pe div_yield =
  let limit = 0.2 in
  let growth = 
    if Float.(<) eps_growth limit then eps_growth else limit 
  in
  (growth +. div_yield) *. 100.0 /. pe

let calc_terminal_value pe growth_of_10y discount =
  let term_val = (pe *. growth_of_10y) /. ((1.0 +. discount) **. 10.0) in
  term_val

let calc_intrinsic_value pe cash_flow growth discount =
  let growth_10y, disc_10y = calc_DFCA cash_flow growth discount in
  let disc_term_val = calc_terminal_value pe growth_10y discount in
  disc_term_val +. disc_10y

let calc_upside cash_flow ttm instrinsic_value =
  (instrinsic_value /. (cash_flow *. ttm)) -. 1.0

let get_intrinsic_price price upside pl_value = 
  let peter_l_ratio = 0.25 *. (pl_value /. 1.5) in
  let dcf_ratio = 0.75 *. (1.0 +. upside) in
  let full_ratio = dcf_ratio +. peter_l_ratio in
  let a = price *. full_ratio in
  a

let rate_stock_price intrinsic_price current_price target =
  let rating = current_price /. intrinsic_price in
  let target_rating = rating /. target in
  (rating, target_rating)

let get_posible_return symbol rating =
  if not (Float.is_nan rating) then (
    let sql = Printf.sprintf "
      Select return
      FROM Returns_of_ratings
      WHERE symbol = \"%s\" and %f >= r_start AND %f < r_end
      " symbol rating rating
    in
    let y_return =
      Sqlite3.prepare Stocks_db.db sql
      |> Stocks_db.fetch_results 
      |> List.hd
    in
  
    match y_return with
    | None -> None
    | Some hd -> 
      List.hd_exn hd |> Sqlite3.Data.to_float
  )
  else None
    
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
        let symbol, rating, discount, price = hd in
        let y_return = get_posible_return symbol rating in
        printf "| %*s" (-6) symbol;
        (
          match y_return with
          | None -> printf "R: %*s%% " (5) "NaN" 
          | Some hd -> printf "R: %*.1f%% " (5) (hd *. 100.0);
        );
        printf "T:";
        printf "%*.0f%% " (4) (discount *. 100.0);
        if Float.(<) (price /. discount) 10.0 then
          printf "%*.3f€ " (5) (price /. discount)
        else if Float.(<) (price /. discount) 1000.0 then
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
let calc_market_cap price shares_outstanding =
  price *. shares_outstanding 

let time_weighted_average ls =
  let rec loop i ls =
    let weigth = (if i < 4 && i > 0 then 3 else if i < 7 then 2 else 1) |> Float.of_int in 
    match ls with
    | [] -> (0.0, weigth)
    | hd :: tl ->
      let s, w = loop (i + 1) tl in  
     (hd *. weigth +. s, weigth +. w)
  in
  let sum, ind = (loop 0 ls) in
  sum /. ind
    
let get_dcf_upside stock_data (new_fcf, old_fcf, years) =
  let ( _,
        industry, 
        sector,
        shares,
        pe,
        price,
        debt,
        tax,
        bond_rate,
        _, _, _) = stock_data in
  let market_cap = calc_market_cap price shares in
  let industry_rating = get_industry_rating industry sector in
  let discount =
    calc_discount market_cap debt tax bond_rate industry_rating 
  in
  let growth = calc_growth new_fcf old_fcf years in
  let intrinsic_value =
    calc_intrinsic_value pe new_fcf growth discount
  in
  calc_upside new_fcf pe intrinsic_value

let rate_stocks ?(filter = "none") stock_data =
  let first_last_financials = Stocks_db.select_first_and_last_fcf () in
  let eps_growth_list = Stocks_db.select_eps_growth () in
  let rec ratings filter stock_data =
    match stock_data with
    | [] -> []
    | hd :: tl ->
      let ( tick_symbol,
            _,
            _,
            _,
            pe,
            price,
            _,
            _,
            _,
            status,
            _,
            div_yield ) =
        hd
      in
      try(
        let filtered_fl_financials =
          List.filter first_last_financials ~f:(
          fun (a, _, _, _) -> String.(=) a tick_symbol) 
        in
        let dcf_upsides = List.map filtered_fl_financials ~f:(
          fun (_, b, c, d) -> get_dcf_upside hd (b, c, d))          
        in
        let dcf_upside_avg = time_weighted_average dcf_upsides in

        let filtered_eps = 
          List.filter eps_growth_list ~f:(
          fun (a, _, _) -> String.(=) a tick_symbol) 
        in
        let pl_values = List.map filtered_eps ~f:(
          fun (_, b, _) ->
          calc_peter_lynch_value b pe div_yield )          
        in
        let pl_value_avg = time_weighted_average pl_values in
        let intrinsic_price = match (Float.is_nan pl_value_avg) with
          | false -> get_intrinsic_price price dcf_upside_avg pl_value_avg
          | true -> get_intrinsic_price price dcf_upside_avg 1.8
        in
        let rating, target_rating = rate_stock_price intrinsic_price price 1.0 in
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
            
           (tick_symbol, rating, target_rating, price) :: ratings filter tl
        else ratings filter tl
      )
      with 
      | a -> 
        let b = Exn.to_string a in
        printf "Error: %s\n" b;
        ratings filter tl
  in
  ratings filter stock_data |> print_price_rating

let stock_ratings filter =
  let data = Stocks_db.select_ratings_data () in
  rate_stocks ~filter data
