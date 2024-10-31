open Base
open Stdio

type t = {
  symbol: string;
  industry_risk: float;
  sector_risk: float;
  shares: float;
  prev_net_income: float;
  ttm_net_income: float;
  prev_cashflow: float;
  ttm_cashflow: float;
  cashflow_growth: float;  
  prev_non_gaap_eps: float;
  ttm_non_gaap_eps: float;
  eps_growth: float;
  date: string;
  price: float;
  base_price: float;
  debt: float;
  tax: float;
  bond_rate: float; 
  div_yield: float
}

let select_ratings_data () =
  let sql =
    Printf.sprintf 
    "
      SELECT
      	symbol
      	, prev_net_income
      	, ttm_net_income
      	, prev_cashflow
      	, ttm_cashflow
      	, cashflow_growth
      	, div_yield
      	, base_price
      	, price
      	, \"date\"
      	, shares
      	, industry_risk
      	, sector_risk
      	, eps_growth
      	, prev_non_gaap_eps
      	, ttm_non_gaap_eps
      	, debt
      	, 0.03 bond_ratio
      	, 0.20 tax
      FROM Backtest_data
      ORDER BY \"date\", symbol
      LIMIT 5000;
      "
  in
  let stmt = Sqlite3.prepare Stocks_db.db sql in
  let sql_data = Stocks_db.fetch_results stmt in
  let rec results data =
    match data with
    | hd :: tl -> (
        match hd with
        |	symbol :: prev_net_income :: ttm_net_income
	        :: prev_cashflow :: ttm_cashflow :: cashflow_growth :: div_yield
	        :: base_price :: price :: date :: shares
        	:: industry :: sector :: eps_growth 
        	:: prev_non_gaap_eps :: ttm_non_gaap_eps
        	:: debt :: tax :: bond_rate :: _  ->
          (try 
            let data : t =
              {
                symbol = Sqlite3.Data.to_string_exn symbol;
                industry_risk = Sqlite3.Data.to_float_exn industry;
                sector_risk = Sqlite3.Data.to_float_exn sector;
                shares = Sqlite3.Data.to_float_exn shares;
                prev_net_income = Sqlite3.Data.to_float_exn prev_net_income;
                ttm_net_income = Sqlite3.Data.to_float_exn ttm_net_income;
                prev_cashflow = Sqlite3.Data.to_float_exn prev_cashflow;
                ttm_cashflow = Sqlite3.Data.to_float_exn ttm_cashflow;
                cashflow_growth = Sqlite3.Data.to_float_exn cashflow_growth;
                prev_non_gaap_eps = Sqlite3.Data.to_float_exn prev_non_gaap_eps;
                ttm_non_gaap_eps = Sqlite3.Data.to_float_exn ttm_non_gaap_eps;
                eps_growth = Sqlite3.Data.to_float_exn eps_growth;
                date = Sqlite3.Data.to_string_exn date;
                price = Sqlite3.Data.to_float_exn price;
                base_price = Sqlite3.Data.to_float_exn base_price;
                debt = Sqlite3.Data.to_float_exn debt;
                tax = Sqlite3.Data.to_float_exn tax;
                bond_rate = Sqlite3.Data.to_float_exn bond_rate;
                div_yield = Sqlite3.Data.to_float_exn div_yield;
               } 
            in
            data :: results tl
          with
          | a -> 
            let b = Exn.to_string a in
            printf "%s has bad data\n" (Sqlite3.Data.to_string_exn symbol);
            printf "Error : %s\n" b;
            results tl)
        | _ -> [])
    | [] -> []
  in
  results sql_data
let calc_pe (t : t) =
  t.base_price /. (t.ttm_net_income /. t.shares)

let calc_dcf_upside (t : t) pe =
  let market_cap = Ratings.calc_market_cap t.base_price t.shares in
  let industry_rating = Ratings.calc_industry_rating t.industry_risk t.sector_risk in
  let discount = Ratings.calc_discount market_cap pe t.debt t.tax t.bond_rate industry_rating in
  let intrinsic_value = Ratings.calc_intrinsic_value pe t.ttm_cashflow t.cashflow_growth discount in
  Ratings.calc_upside t.ttm_cashflow pe intrinsic_value  

let backtest () =
  let ratings_data = select_ratings_data () in
  let rec while_symbol symbol date stock_data =  
    match stock_data with
    | [] -> ([], stock_data)
    | (hd : t) :: tl when String.(=) hd.symbol symbol
                && String.(=) hd.date date ->
      let same, rest = while_symbol date symbol tl in
      (hd :: same, rest)
    | _  -> ([], stock_data)
  in
  let rec loop stock_data =
    match stock_data with
    | [] -> ()
    | (hd : t) :: tl -> 
      let same_symbols, rest_of_list = while_symbol hd.symbol hd.date tl in
      let data_set : (t list) = hd :: same_symbols in
      let pe = calc_pe hd in
      let dcf_upsides = List.map data_set ~f:(
        fun (x : t) -> calc_dcf_upside x pe
      )          
      in
      let dcf_upside_avg =
        List.fold dcf_upsides ~init:0.0 ~f:(+.)
        /. (Float.of_int (List.length dcf_upsides))
      in
      let pl_values = List.map data_set ~f:(
        fun (x : t) ->
         Ratings.calc_peter_lynch_value x.eps_growth pe x.div_yield )          
      in
      let pl_value_avg =
        List.fold pl_values ~init:0.0 ~f:(+.) 
        /. (Float.of_int (List.length pl_values))
      in
      let intrinsic_price =
        Ratings.get_intrinsic_price hd.price dcf_upside_avg pl_value_avg
       in
      let rating, target_rating =
         Ratings.rate_stock_price intrinsic_price hd.price 1.0
      in
      printf "%*s: %*.2f %*.2f" 7 hd.symbol 7 rating 7 target_rating;
      loop rest_of_list
  in
  loop ratings_data;
