open Base
open Stdio

let first_last_financials = Stocks_db.select_first_and_last_fcf ()

let calc_eq_discount ?(expected_return = 0.15) pe market_cap debt =
  let ep_disc = (300.0 +. pe) /. 315.0 in
  let eq_disc = market_cap /. (market_cap +. debt) *. expected_return in
  ep_disc *. eq_disc

let calc_debt_discount market_cap debt tax bond_rate =
  debt /. market_cap *. bond_rate *. (1.0 -. tax)

let calc_growth ticker_symbol =
  let _, old_fcf, new_fcf = List.find_exn first_last_financials ~f:(
    fun (a, _, _) -> String.(=) a ticker_symbol)
  in
  let increase = (new_fcf -. old_fcf) /. (Float.abs old_fcf) +. 1.0  in
  increase **. 0.25 -. 1.0
  
let get_industry_rating industry sector =
  let industry_risk, sector_risk =
    Stocks_db.select_sector_and_industry sector industry
  in
  let industry_and_sector_risk =
    (industry_risk +. sector_risk) /. 3.0 +. 0.67
  in
  industry_and_sector_risk 

let calc_discount market_cap pe debt tax bond_rate
    industry_rating =
  let eq_disc = calc_eq_discount pe market_cap debt in
  let debt_disc = calc_debt_discount market_cap debt tax bond_rate in
  (eq_disc +. debt_disc) *. industry_rating

let  calc_DFCA cash_flow growth discount =
  let rec loop year limiter prev_growth= 
    (match year with
    | 11.0 -> (0.0, 0.0)
    | year ->
      let multiplier = 0.95 -. Float.abs(growth) **. 0.5 /. 6.0 in
      let current_year_growth = prev_growth *. (if Float.(<) limiter 0.0 then multiplier else 1.0) in
      let cash_flow_growth =
        cash_flow *. ((1.0 +. current_year_growth) **. year)
      in
      let cash_flow_discounted =
        cash_flow_growth /. ((1.0 +. discount) **. year)
      in
      let new_limiter = limiter -. Float.abs (growth *. year) in
      let x, y = loop (year +. 1.0) new_limiter current_year_growth in
      (cash_flow_growth +. x, cash_flow_discounted +. y))
  in loop 0.0 1.0 growth 

let calc_terminal_value pe growth_of_10y discount =
  let term_val = pe *. growth_of_10y in
  (term_val, term_val /. ((1.0 +. discount) **. 10.0))

let calc_intrinsic_value pe cash_flow growth discount =
  let growth_10y, disc_10y = calc_DFCA cash_flow growth discount in
  let _, disc_term_val = calc_terminal_value pe growth_10y discount in
  disc_term_val +. disc_10y

let calc_upside market_cap ttm instrinsic_value =
  (instrinsic_value /. (market_cap *. ttm)) -. 1.0

let get_intrinsic_price price upside = price *. (1.0 +. upside)

let rate_stock_price intrinsic_price current_price target =
  current_price /. intrinsic_price /. (Float.of_int target) *. 100.0

let print_price_rating ratings =
  let max_col = 5 in
  let row_len = 27 in
  let rec seperate_row col =
    if col >= 1 then (
      if col = max_col then printf "+";
      String.make (row_len - 2) '-' |> printf "%s+";
      seperate_row (col - 1))
    else print_endline ""
  in
  seperate_row max_col;
  let rec print ratings max_col col =
    match ratings with
    | hd :: tl ->
        let ticker_symbol, discount, price = hd in
        printf "| %*s" (-5) ticker_symbol;
        printf " %*.0f%% " (5) (discount *. 100.0);
        printf " %*.1f Eur " (5) (price /. discount);
        if col = 1 then (
          print_endline "|";
          seperate_row max_col;
          print tl max_col max_col)
        else print tl max_col (col - 1)
    | [] ->
        if max_col - col > 0 then
        (print_endline "|";
        printf "+";
        max_col - col |> seperate_row)
  in
  print ratings max_col max_col

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
              free_cash_flow,
              tax,
              bond_rate,
              status,
              target ) =
          hd
        in
        let industry_rating = get_industry_rating industry sector in
        let discount =
          calc_discount market_cap pe debt tax bond_rate industry_rating 
        in
        let growth = calc_growth tick_symbol in
        let intrinsic_value =
          calc_intrinsic_value pe free_cash_flow growth discount
        in
        let upside = calc_upside market_cap pe intrinsic_value in
        let intrinsic_price = get_intrinsic_price price upside in
        let price_discount = rate_stock_price intrinsic_price price target in
        let filter_under price_discount treshold =
          if
            Float.( < ) price_discount treshold
            && Float.( > ) price_discount 0.0
          then true
          else false
        in
        let is_printable =
          match filter with
          | "none" -> true
          | "under" -> filter_under price_discount 1.0
          | "fair" -> filter_under price_discount 1.2
          | h -> String.(=) h (String.lowercase status) 
        in
        if is_printable then
          [ (tick_symbol, price_discount, price) ] @ ratings filter tl
        else ratings filter tl
  in
  ratings filter stock_data |> print_price_rating

let stock_ratings filter =
  let data = Stocks_db.select_ratings_data () in
  rate_stocks ~filter data
