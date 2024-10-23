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
      | _ -> Int.of_string s)
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
  compute ~time:8.0 ~f:get >>= function
  | `Timeout ->
      print_endline "Timeout";
      return default
  | `Done (resp, body) -> (
      let code = resp |> Response.status |> Code.code_of_status in
      match code with
      | 200 -> body |> Cohttp_lwt.Body.to_string >|= fun body -> f body
      | _ -> body |> Cohttp_lwt.Body.to_string >|= fun _ -> default)

let get_market_cap_and_ep ticker_symbol =
  let end_point = "OVERVIEW" in
  let f body =
    let market_cap = extract_from_json body  "MarketCapitalization" to_int_exn in
    let pe = extract_from_json body "TrailingPE"  to_float_exn in
    (market_cap, pe)
  in
  Lwt_main.run (api_call f end_point ticker_symbol (0, 0.0))

let get_price ticker_symbol =
  let end_point = "GLOBAL_QUOTE" in
  let f body = extract_from_json ~key:"Global Quote" body "05. price" to_float_exn in
    Lwt_main.run (api_call f end_point ticker_symbol (0.0)) 

let update_price ticker_symbol =
  let price = get_price ticker_symbol in
  let cap, ep = get_market_cap_and_ep ticker_symbol in
  Stocks_db.update_price ticker_symbol (price, cap, ep)

let update_stock ticker_symbol =
  let end_point = "OVERVIEW"  in
  let f body = body in
  let data = Lwt_main.run (api_call f end_point ticker_symbol "") in
  let ticker_symbol_2 = extract_from_json data "Symbol" to_string in
  let stock_exits = String.( = ) ticker_symbol ticker_symbol_2 in
  match stock_exits with
  | true ->
      let price = get_price ticker_symbol in
      let beta = extract_from_json data "Beta" to_float_exn in
      let market_cap = extract_from_json data "MarketCapitalization" to_int_exn in
      let currency = extract_from_json data "Currency"  to_string in
      let industry = extract_from_json data "Industry" to_string in
      let sector = extract_from_json data "Sector" to_string in
      let country = extract_from_json data "Country" to_string in
      Stocks_db.update_stock ticker_symbol
        (price, beta, market_cap, currency, industry, sector, country, "TRUE")
  | false -> print_endline "Cant access stock"

let update_financials ticker_symbol =
  let f body = body in
  let income_end_point = "INCOME_STATEMENT" in
  let balance_end_point = "BALANCE_SHEET" in
  let cashflow_end_point = "CASH_FLOW" in
  let income_json = Lwt_main.run (api_call f income_end_point ticker_symbol "") in
  let balance_json = Lwt_main.run (api_call f balance_end_point ticker_symbol "") in
  let cashflow_json = Lwt_main.run (api_call f cashflow_end_point ticker_symbol "") in
  let cash =
    extract_from_json_list ~key:"annualReports" balance_json "cashAndCashEquivalentsAtCarryingValue" 0 to_int_exn
  in
  let currency =
    extract_from_json_list ~key:"annualReports" balance_json "reportedCurrency" "USD" to_string
  in
  let assests =
    extract_from_json_list ~key:"annualReports" balance_json "totalAssets" 0 to_int_exn
  in
  let debt = extract_from_json_list ~key:"annualReports" balance_json "currentDebt" 0 to_int_exn in
  let year = extract_from_json_list ~key:"annualReports" income_json "fiscalDateEnding" "" to_string in
  let revenue = extract_from_json_list ~key:"annualReports" income_json "totalRevenue" 0 to_int_exn in
  let net_income =
    extract_from_json_list ~key:"annualReports" income_json "netIncome" 0 to_int_exn
  in
  let cash_flow =
    extract_from_json_list ~key:"annualReports" cashflow_json "operatingCashflow" 0 to_int_exn
  in
  let min_arr_length = min (Array.length cash) (Array.length cash_flow) |> min (Array.length net_income)in
  match Stocks_db.delete_financials ticker_symbol with
  | true ->
      for i = 0 to min_arr_length - 1 do
        Stocks_db.update_financials ticker_symbol
          ( year.(i),
            "FY",
            revenue.(i),
            net_income.(i),
            cash.(i),
            assests.(i),
            debt.(i),
            cash_flow.(i),
            currency.(i) )
      done;
      print_endline "Financials updated";
  | false -> print_endline "something went wrong"
  

let update_all_prices () =
  let symbols = Stocks_db.select_stocks_symbols () in
  List.iter symbols ~f:(fun symbol -> update_price symbol)

let update_forex () =
  let currencies = Stocks_db.select_currencies () in
  let rec loop currencies =
    match currencies with
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
      [(price, name)] @ loop tl
    | [] -> []
  in
  let names_prices_list = loop currencies in
  let names = List.map names_prices_list ~f:(fun (_,x) -> x ) in
  let prices = List.map names_prices_list ~f:(fun (x,_) -> x ) in
  Stocks_db.insert_forex names prices

let update_data () =
  let symbols = Stocks_db.select_stocks_symbols () in
  List.iter symbols ~f:(fun symbol ->
      update_financials symbol;
      update_price symbol);
  update_forex ()
