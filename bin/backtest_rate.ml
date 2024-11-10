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
  debt: float;
  tax: float;
  bond_rate: float; 
  div_yield: float
}

let fetch_backtest_data () =
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
      ORDER BY \"date\", symbol;
      "
  in
  let stmt = Sqlite3.prepare Stocks_db.db sql in
  let open Sqlite3 in
  let rec f accum =
    match Sqlite3.step stmt with
    | Rc.DONE ->
        finalize stmt |> ignore;
        List.rev accum
    | Rc.ROW -> 
      let result = Array.to_list (row_data stmt) in
      (
        match result with
        |	symbol :: prev_net_income :: ttm_net_income
          :: prev_cashflow :: ttm_cashflow :: cashflow_growth :: div_yield
          :: price :: date :: shares
        	:: industry :: sector :: eps_growth 
        	:: prev_non_gaap_eps :: ttm_non_gaap_eps
        	:: debt :: tax :: bond_rate :: _  ->
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
              debt = Sqlite3.Data.to_float_exn debt;
              tax = Sqlite3.Data.to_float_exn tax;
              bond_rate = Sqlite3.Data.to_float_exn bond_rate;
              div_yield = Sqlite3.Data.to_float_exn div_yield;
             } 
          in
          f (data :: accum)
        | _ -> f accum
      )
    | Rc.ERROR -> failwith (Sqlite3.Rc.to_string Rc.ERROR)
    | _ -> failwith "Unexpected result code"
  in
  f []

let calc_pe (t : t) =
  t.price /. (t.ttm_net_income /. t.shares)

let calc_dcf_upside (t : t) pe =
  let market_cap = Ratings.calc_market_cap t.price t.shares in
  let industry_rating = Ratings.calc_industry_rating t.industry_risk t.sector_risk in
  let discount = Ratings.calc_discount market_cap pe t.debt t.tax t.bond_rate industry_rating in
  let intrinsic_value = Ratings.calc_intrinsic_value pe t.ttm_cashflow t.cashflow_growth discount in
  Ratings.calc_upside t.ttm_cashflow pe intrinsic_value 

let rating_to_sql_values (t : t) rating comma =
  let sql_values = Printf.sprintf "
    (\"%s\",\"%s\",%.8f) "
    t.symbol t.date rating
  in   
  match comma with
  | true -> sql_values ^ ","
  | _ -> sql_values
    
let insert_ratings results_sql  =
  let base_sql =  
     "INSERT INTO Backtest_ratings (symbol, \"date\", rating)\n
       VALUES 
       " 
  in
  let sql = base_sql ^ results_sql in
  let inserted = Sqlite3.exec Stocks_db.db sql |> Sqlite3.Rc.to_string in
  match inserted with
  | "OK" ->  ();
  | _ -> ()

let delete_ratings () =
  let sql =
    Printf.sprintf "DELETE FROM Backtest_ratings" 
  in
  let deleted = Sqlite3.exec Stocks_db.db sql |> Sqlite3.Rc.to_string in
  match deleted with
  | "OK" -> ()
  | code -> printf "Error deleting backtest ratings: %s\n%!" code

let rate_stocks ratings_data =
  let data_length = List.length ratings_data |> Float.of_int in
  let data_left = ref data_length in
  let rating_sql = Buffer.create (50000 * 38) in 
  let rating_count = ref 0 in
  printf "\r%.1f%% %!    " 0.0;
  
  let rec while_symbol symbol date stock_data =  
    match stock_data with
    | [] -> ([], stock_data)
    | (hd : t) :: tl
      when String.(=) hd.symbol symbol
      && String.(=) hd.date date ->
      let same, rest = while_symbol symbol date tl in
      (hd :: same, rest)
    | _  -> ([], stock_data)
  in
  
  let rec loop stock_data =
    match stock_data with
    | [] -> ()
    | (hd : t) :: tl -> 
      let same_symbols, rest_of_list = while_symbol hd.symbol hd.date tl in
      let data_set : (t list) = hd :: same_symbols in
      let data_set_length = List.length data_set |> Float.of_int in
      let pe = calc_pe hd in
      let dcf_upsides = List.map data_set ~f:(
        fun (x : t) -> calc_dcf_upside x pe
      )          
      in
      let dcf_upside_avg =
        List.fold dcf_upsides ~init:0.0 ~f:(+.)
        /. data_set_length
      in
      let pl_values = List.map data_set ~f:(
        fun (x : t) ->
         Ratings.calc_peter_lynch_value x.eps_growth pe x.div_yield )          
      in
      let pl_value_avg =
        List.fold pl_values ~init:0.0 ~f:(+.) 
        /. data_set_length
      in
      let intrinsic_price =
        Ratings.get_intrinsic_price hd.price dcf_upside_avg pl_value_avg
       in
      let rating, _ =
         Ratings.rate_stock_price intrinsic_price hd.price 1.0
      in

      
      data_left := !data_left -. data_set_length; 
      rating_count := !rating_count + 1;
      if Int.(=) !rating_count 50000 || Float.(=) !data_left 0.0 then
      (
        Buffer.add_string rating_sql (rating_to_sql_values hd rating false);
        rating_count := 0;
        insert_ratings (Buffer.contents rating_sql); 
        Buffer.clear rating_sql
      )
      else
      (
        Buffer.add_string rating_sql (rating_to_sql_values hd rating true);
      );
      printf "\r%.1f%% %!        " (100.0 -. !data_left /. data_length *. 100.0); 
      loop rest_of_list
  in
  
  loop ratings_data

let backtest () =
  let start_time = Unix.gettimeofday () in
  print_endline "Extracting data";
  delete_ratings ();
  let ratings_data = fetch_backtest_data () in
  printf "\rData extracted in %.0f seconds\n%!" (Unix.gettimeofday () -. start_time);

  let start_time = Unix.gettimeofday () in
  rate_stocks ratings_data;
  printf "\rBacktest stocks rating completed in %.0f seconds\n%!" (Unix.gettimeofday () -. start_time)
