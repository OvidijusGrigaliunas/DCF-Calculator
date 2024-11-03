open Base
open Cohttp_lwt_unix
open Lwt
open Cohttp
open Stdio
open Yojson.Basic.Util

let compute ~time ~f =
  Lwt.pick
    [
      (f () >|= fun v -> `Done v); (Lwt_unix.sleep time >|= fun () -> `Timeout);
    ]

let to_int_exn = function
  | `Float f -> Int.of_float f
  | `String s ->
    (match s with
      | "None" -> 0
      | _ -> Float.of_string s |> Int.of_float)
  | `Int i -> i
  | _ -> failwith "Expected float or int"

let to_float_exn = function
  | `Float f -> f
  | `String s -> 
    (match s with
      | "None" -> 0.0
      | _ -> Float.of_string s)
  | `Int i -> Float.of_int i
  | _ -> failwith "Expected float or int"

let extract_from_json_list ?(key = "") data filter default f =
  let json = Yojson.Basic.from_string data in
  let obj = 
    match key with
    | "" -> to_list json
    | key -> Yojson.Basic.Util.(member key json) |> to_list in
  let results = List.map obj ~f:(fun x -> member filter x |> f) in
  match results with
  | [] -> Array.of_list [ default ]
  | _ -> Array.of_list results

let extract_from_json ?(key = "") data filter f =
  let json = Yojson.Basic.from_string data in
  match key with
  | "" ->  member filter json |> f
  | key -> let obj = Yojson.Basic.Util.(member key json) in
  member filter obj |> f

let api_call f endpoint symbol default =
  let api_function = Printf.sprintf
    "https://www.alphavantage.co/query?function=%s" endpoint
  in
  let url =
    match symbol with 
    | "" -> Printf.sprintf "%s&apikey=%s" api_function Secrets.api_key
    | _ -> Printf.sprintf "%s&symbol=%s&apikey=%s" api_function symbol Secrets.api_key
  in
  let get () = Client.get (Uri.of_string url) in
  compute ~time:60.0 ~f:get >>= function
  | `Timeout ->
      print_endline "Timeout";
      return default
  | `Done (resp, body) -> (
      let code = resp |> Response.status |> Code.code_of_status in
      match code with
      | 200 -> body |> Cohttp_lwt.Body.to_string >|= fun body -> f body
      | _ -> body |> Cohttp_lwt.Body.to_string >|= fun _ -> default)

let get_price ticker_symbol =
  let end_point = "GLOBAL_QUOTE" in
  let f body = extract_from_json ~key:"Global Quote" body "05. price" to_float_exn in
  Lwt_main.run (api_call f end_point ticker_symbol (0.0)) 

let update_price ticker_symbol =
  let price = get_price ticker_symbol in
  Stocks_db.update_price ticker_symbol price

let update_stock ticker_symbol =
  let end_point = "OVERVIEW"  in
  let f body = body in
  let data = Lwt_main.run (api_call f end_point ticker_symbol "") in
  let ticker_symbol_2 = extract_from_json data "Symbol" to_string in
  let stock_exits = String.( = ) ticker_symbol ticker_symbol_2 in
  match stock_exits with
  | true ->
      let beta = extract_from_json data "Beta" to_float_exn in
      let shares = extract_from_json data "SharesOutstanding" to_int_exn in
      let currency = extract_from_json data "Currency"  to_string in
      let div_yield = extract_from_json data "DividendYield" to_float_exn in
      let industry = extract_from_json data "Industry" to_string in
      let sector = extract_from_json data "Sector" to_string in
      let country = extract_from_json data "Country" to_string in
      Stocks_db.update_stock ticker_symbol
         (beta, div_yield, shares, currency, industry, sector, country, "TRUE")
  | false -> print_endline "Cant access stock"

let extract_data_fin key (income_json, balance_json, cashflow_json) =
  let cash =
    extract_from_json_list ~key:key balance_json "cashAndCashEquivalentsAtCarryingValue" 0 to_int_exn
  in
  let currency =
    extract_from_json_list ~key:key balance_json "reportedCurrency" "USD" to_string
  in
  let assests =
    extract_from_json_list ~key:key balance_json "totalAssets" 0 to_int_exn
  in
  let debt = extract_from_json_list ~key:key balance_json "currentDebt" 0 to_int_exn in
  let year = extract_from_json_list ~key:key income_json "fiscalDateEnding" "" to_string in
  let revenue = extract_from_json_list ~key:key income_json "totalRevenue" 0 to_int_exn in
  let net_income =
    extract_from_json_list ~key:key income_json "netIncome" 0 to_int_exn
  in
  let cash_flow =
    extract_from_json_list ~key:key cashflow_json "operatingCashflow" 0 to_int_exn
  in
  let time = match key with
    | "annualReports" -> "FY"
    | _ -> "FQ"
  in 
  let min_arr_length = min (Array.length cash) (Array.length cash_flow) |> min (Array.length net_income) in
  let rec loop i max_i =
    if max_i > i then (
       let financial = (year.(i),
          time,
          net_income.(i),
          cash_flow.(i),
          revenue.(i),
          cash.(i),
          assests.(i), 
          debt.(i),
          currency.(i)) in
        financial :: loop (i + 1) max_i
        )
      else []
  in
  loop 0 min_arr_length 

let extract_data_eps key earnings_json =
    let time = match key with
      | "annualEarnings" -> "FY"
      | _ -> "FQ"
    in 
    let eps =
      extract_from_json_list ~key:key earnings_json "reportedEPS" 0.0 to_float_exn
    in
    let year =
      extract_from_json_list ~key:key earnings_json "fiscalDateEnding" "" to_string
    in
    let arr_length = Array.length eps in
    let rec loop i max_i =
      if max_i > i then (
         let financial = (year.(i),
            time,
            eps.(i)) in
          financial :: loop (i + 1) max_i
          )
        else []
    in
    loop 0 arr_length
  
let extract_data_div dividends_json =
    let amount =
      extract_from_json_list ~key:"data" dividends_json "amount" 0.0 to_float_exn
    in
    let year =
      extract_from_json_list ~key:"data" dividends_json "ex_dividend_date" "" to_string
    in
    let arr_length = Array.length amount in
    let rec loop i max_i =
      if max_i > i then (
         let dividends =
           (year.(i),
            amount.(i)) in
          dividends :: loop (i + 1) max_i
          )
        else []
    in
    loop 0 arr_length

let get_earnings ticker_symbol =
  let f body = body in
  let end_point = "EARNINGS" in
  Lwt_main.run (api_call f end_point ticker_symbol "") 

let get_dividends ticker_symbol =
  let f body = body in
  let end_point = "DIVIDENDS" in
  Lwt_main.run (api_call f end_point ticker_symbol "") 

let get_splits ticker_symbol = 
  let f body = body in
  let end_point = "SPLITS" in
  Lwt_main.run (api_call f end_point ticker_symbol "") 

let extract_data_splits splits_json =
    let amount =
      extract_from_json_list ~key:"data" splits_json "split_factor" 0.0 to_float_exn
    in
    let year =
      extract_from_json_list ~key:"data" splits_json "effective_date" "" to_string
    in
    let arr_length = Array.length amount in
    let rec loop i max_i =
      if max_i > i then (
         (year.(i), amount.(i)) :: loop (i + 1) max_i
        )
      else []
    in
    loop 0 arr_length
  
let update_splits () =
  let symbols = Stocks_db.select_stocks_symbols () in
  let total = List.length symbols in
  List.iteri symbols ~f:
    (fun index symbol ->
     printf "\r%*s (%d/%d)" (-7) symbol (index + 1) total;
     Stdio.Out_channel.flush stdout;
     get_splits symbol
     |> extract_data_splits
     |> Stocks_db.update_splits symbol; 
     Thread.delay 0.2
     );
  printf "\r\n";
  Stdio.Out_channel.flush stdout

let extract_json_his_price price_json  =
  let json = Yojson.Basic.from_string price_json in
  let data_list = Yojson.Basic.Util.(member "Time Series (Daily)" json) |> to_assoc in
  List.map data_list ~f:(
    fun (date, data) ->
      let close = member "4. close" data |> to_float_exn in
      (date, close)
    )

let get_historical_prices ticker_symbol =
  let f body = body in
  let end_point = "TIME_SERIES_DAILY&outputsize=full" in
  Lwt_main.run (api_call f end_point ticker_symbol "") 

let update_history_prices () =
  let symbols = Stocks_db.select_stocks_symbols () in
  let total = List.length symbols in
  List.iteri symbols ~f:
    (fun index symbol ->
     printf "\r%*s (%d/%d)" (-7) symbol (index + 1) total;
     Stdio.Out_channel.flush stdout;
     get_historical_prices symbol
     |> extract_json_his_price
     |> Stocks_db.update_history_prices symbol);
  printf "\r\n";
  Stdio.Out_channel.flush stdout

let get_financials ticker_symbol =
  let f body = body in
  let income_end_point = "INCOME_STATEMENT" in
  let balance_end_point = "BALANCE_SHEET" in
  let cashflow_end_point = "CASH_FLOW" in
  let income_json = Lwt_main.run (api_call f income_end_point ticker_symbol "") in
  let balance_json = Lwt_main.run (api_call f balance_end_point ticker_symbol "") in
  let cashflow_json = Lwt_main.run (api_call f cashflow_end_point ticker_symbol "") in
  (income_json, balance_json, cashflow_json)

let update_fundamentals ticker_symbol =
  let financials = get_financials ticker_symbol in
  let earnings = get_earnings ticker_symbol in
  let financals_data_qrt = extract_data_fin "quarterlyReports" financials in
  let financals_data_yearly = extract_data_fin "annualReports" financials in
  let eps_data_qrt = extract_data_eps "quarterlyEarnings" earnings in
  let eps_data_yearly = extract_data_eps "annualEarnings" earnings in
  let dividends = get_dividends ticker_symbol |> extract_data_div in
  match Stocks_db.delete_financials ticker_symbol with
  | true ->
    Stocks_db.update_financials ticker_symbol (List.append financals_data_qrt financals_data_yearly);
    Stocks_db.update_earnings ticker_symbol (List.append eps_data_qrt eps_data_yearly);
    Stocks_db.update_dividends ticker_symbol dividends
  | false -> print_endline "Something went wrong"
  
let update_all_prices () =
  let symbols = Stocks_db.select_stocks_symbols () in
  let total = List.length symbols in
  let start = Unix.gettimeofday () in
  List.iteri symbols ~f:
    (fun index symbol ->
     printf "\r%*s (%d/%d)" (-7) symbol (index + 1) total;
     Stdio.Out_channel.flush stdout;
     update_price symbol);
  let end_time = Unix.gettimeofday () in
  printf "\rTotal time: %.2f\n" (end_time -. start);
  Stdio.Out_channel.flush stdout
  
let update_forex () =
  Stocks_db.clean_up_financials_currency ();
  let currencies = Stocks_db.select_currencies () in
  let rec loop currencies =
    match currencies with
    | "None" :: tl -> loop tl
    | hd :: tl ->
      let end_point = Printf.sprintf "CURRENCY_EXCHANGE_RATE&from_currency=%s&to_currency=EUR" hd in
      let f body = body in
      let data = Lwt_main.run (api_call f end_point "" "") in
      let name =
        extract_from_json ~key:"Realtime Currency Exchange Rate" data "1. From_Currency Code" to_string
      in
      let price =
        extract_from_json ~key:"Realtime Currency Exchange Rate" data "5. Exchange Rate" to_float_exn
      in     
      (price, name) :: loop tl
    | [] -> []
  in
  let names_prices_list = loop currencies in
  let names = List.map names_prices_list ~f:(fun (_,x) -> x ) in
  let prices = List.map names_prices_list ~f:(fun (x,_) -> x ) in
  Stocks_db.insert_forex names prices

let update_data () =
  let symbols = Stocks_db.select_stocks_symbols () in
  let total = List.length symbols in
  let start = Unix.gettimeofday () in
  printf "%*s (%d/%d)" (-7) "" (0) total;
  List.iteri symbols ~f:(fun index symbol ->
    let time = Unix.gettimeofday () in
    Stdio.Out_channel.flush stdout;
    update_stock symbol;
    update_fundamentals symbol;
    update_price symbol;
    let dur = Unix.gettimeofday () -. time in
    printf "\r%*s (%d/%d) %*.2f s" (-7) symbol (index + 1) total (7) dur
  );
  let end_time = Unix.gettimeofday () in
  printf "\rTotal time: %.2f\n" (end_time -. start);
  Stdio.Out_channel.flush stdout;
  update_forex ()
