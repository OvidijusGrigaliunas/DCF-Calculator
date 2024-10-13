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
  | `Int i -> i
  | _ -> failwith "Expected float or int"

let to_float_exn = function
  | `Float f -> f
  | `Int i -> Float.of_int i
  | _ -> failwith "Expected float or int"

let extract_from_json_list data filter default f =
  let json = Yojson.Basic.from_string data |> to_list in
  let results = List.map json ~f:(fun x -> member filter x |> f) in
  match results with
  | [] -> Array.of_list [ default ]
  | _ -> Array.of_list results

let extract_from_json data filter default f =
  let json = Yojson.Basic.from_string data |> to_list in
  match json with [] -> default | hd :: _ -> member filter hd |> f

let api_call f endpoint default =
  let url =
    "https://financialmodelingprep.com/api/v3/" ^ endpoint ^ Secrets.api_key
  in
  let get () = Client.get (Uri.of_string url) in
  compute ~time:5.0 ~f:get >>= function
  | `Timeout ->
      print_endline "Timeout";
      return default
  | `Done (resp, body) -> (
      let code = resp |> Response.status |> Code.code_of_status in
      match code with
      | 200 -> body |> Cohttp_lwt.Body.to_string >|= fun body -> f body
      | _ -> body |> Cohttp_lwt.Body.to_string >|= fun _ -> default)

let update_price ticker_symbol =
  let end_point = "quote-order/" ^ ticker_symbol ^ "?" in
  let f body =
    let price = extract_from_json body "price" 0.0 to_float_exn in
    let market_cap = extract_from_json body "marketCap" 0 to_int_exn in
    let pe = extract_from_json body "pe" 0.0 to_float_exn in
    (price, market_cap, pe)
  in
  let price_data = Lwt_main.run (api_call f end_point (0.0, 0, 0.0)) in
  Stocks_db.update_price ticker_symbol price_data

let update_stock ticker_symbol =
  let end_point = "profile/" ^ ticker_symbol ^ "?" in
  let f body = body in
  let data = Lwt_main.run (api_call f end_point "") in
  let ticker_symbol_2 = extract_from_json data "symbol" "" to_string in
  let stock_exits = String.( = ) ticker_symbol ticker_symbol_2 in
  match stock_exits with
  | true ->
      let price = extract_from_json data "price" 0.0 to_float_exn in
      let beta = extract_from_json data "beta" 0.0 to_float_exn in
      let market_cap = extract_from_json data "mktCap" 0 to_int_exn in
      let currency = extract_from_json data "currency" "" to_string in
      let industry = extract_from_json data "industry" "" to_string in
      let sector = extract_from_json data "sector" "" to_string in
      let country = extract_from_json data "country" "" to_string in
      Stocks_db.update_stock ticker_symbol
        (price, beta, market_cap, currency, industry, sector, country, "TRUE")
  | false -> print_endline "Cant access stock"
(* TODO: add manual *)

let update_financials ticker_symbol =
  let f body = body in
  let end_point = ticker_symbol ^ "?period=annual&" in
  let income_end_point = "income-statement/" ^ end_point in
  let balance_end_point = "balance-sheet-statement/" ^ end_point in
  let cashflow_end_point = "cash-flow-statement/" ^ end_point in
  let income_json = Lwt_main.run (api_call f income_end_point "") in
  let balance_json = Lwt_main.run (api_call f balance_end_point "") in
  let cashflow_json = Lwt_main.run (api_call f cashflow_end_point "") in
  let cash =
    extract_from_json_list balance_json "cashAndCashEquivalents" 0 to_int_exn
  in
  let currency =
    extract_from_json_list balance_json "reportedCurrency" "USD" to_string
  in
  let assests =
    extract_from_json_list balance_json "totalAssets" 0 to_int_exn
  in
  let debt = extract_from_json_list balance_json "totalDebt" 0 to_int_exn in
  let year = extract_from_json_list income_json "calendarYear" "" to_string in
  let period = extract_from_json_list income_json "period" "" to_string in
  let revenue = extract_from_json_list income_json "revenue" 0 to_int_exn in
  let net_income =
    extract_from_json_list income_json "netIncome" 0 to_int_exn
  in
  let eps = extract_from_json_list income_json "eps" 0.0 to_float_exn in
  let rnd =
    extract_from_json_list income_json "researchAndDevelopmentExpenses" 0
      to_int_exn
  in
  let free_cash_flow =
    extract_from_json_list cashflow_json "freeCashFlow" 0 to_int_exn
  in
  match Stocks_db.delete_financials ticker_symbol with
  | true ->
      for i = 0 to Array.length cash - 1 do
        Stocks_db.update_financials ticker_symbol
          ( year.(i),
            period.(i),
            revenue.(i),
            net_income.(i),
            eps.(i),
            rnd.(i),
            cash.(i),
            assests.(i),
            debt.(i),
            free_cash_flow.(i),
            currency.(i) )
      done
  | false -> print_endline "something went wrong"
  

let update_all_prices () =
  let symbols = Stocks_db.select_stocks_symbols () in
  List.iter symbols ~f:(fun symbol -> update_price symbol)

let update_forex () =
  let currencies = Stocks_db.select_currencies () in
  let rec loop currencies =
    match currencies with
    | hd :: tl ->
      let end_point = "quote/" ^ hd ^ "EUR?" in
      let f body = body in
      let data = Lwt_main.run (api_call f end_point "") in
      let name =
        extract_from_json data "name" "" to_string
      in
      let price =
        extract_from_json data "price" 0.0 to_float_exn
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
