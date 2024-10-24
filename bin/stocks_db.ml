open Base
open Stdio

let db = Secrets.current_dir ^ "stocks" |> Sqlite3.db_open

let rec print_results a =
  match a with
  | [] -> printf ""
  | hd :: tl -> (
      match hd with
      | Some str ->
          print_endline str;
          print_results tl
      | None -> print_results tl)

let row_exists table identifier value =
  let sql =
    Printf.sprintf "SELECT * FROM %s WHERE %s = \"%s\";" table identifier value
  in
  let result = Sqlite3.prepare db sql |> Sqlite3.step in
  match result with Sqlite3.Rc.ROW -> true | _ -> false

let fetch_results stmt =
  let open Sqlite3 in
  let rec f accum =
    match Sqlite3.step stmt with
    | Rc.DONE ->
        finalize stmt |> ignore;
        List.rev accum
    | Rc.ROW -> f (Array.to_list (row_data stmt) :: accum)
    | Rc.ERROR -> failwith (Sqlite3.Rc.to_string Rc.ERROR)
    | _ -> failwith "Unexpected result code"
  in
  f []

let select_sector_and_industry sector industry =
  let sql_in =
    Printf.sprintf "SELECT risk FROM Industries WHERE industry = \"%s\";"
      industry
  in
  let sql_sec =
    Printf.sprintf "SELECT risk FROM Sectors WHERE sector = \"%s\";" sector
  in
  let sec_risk =
    Sqlite3.prepare db sql_sec |> fetch_results |> List.hd_exn |> List.hd_exn
    |> Sqlite3.Data.to_float_exn
  in
  let in_risk =
    Sqlite3.prepare db sql_in |> fetch_results |> List.hd_exn |> List.hd_exn
    |> Sqlite3.Data.to_float_exn
  in
  (sec_risk, in_risk)

let select_stocks_symbols ?(pull=true) () =
  let pull_str = match pull with
  | true -> "TRUE"
  | false -> "FALSE"
  in
  let sql = Printf.sprintf "SELECT symbol FROM Stocks WHERE pullable = \"%s\"" pull_str in
  let stmt = Sqlite3.prepare db sql in
  let sql_data = fetch_results stmt in
  let rec results data =
    match data with
    | hd :: tl -> (
        match hd with
        | symbol :: _ -> [ Sqlite3.Data.to_string symbol ] @ results tl
        | [] -> [])
    | [] -> []
  in
  let rec clean_up data =
    match data with Some hd :: tl -> [ hd ] @ clean_up tl | _ -> []
  in
  results sql_data |> clean_up

let insert_industry industry =
  let exists = row_exists "Industries" "industry" industry in
  match exists with
  | true -> ()
  | false -> (
      let risk =
        printf
          "Industry doesn't exist in database\n\
           Please write estimated security of %s industry (0-10)\n\
           %!"
          industry;
        let input = In_channel.input_line In_channel.stdin in

        match input with Some line -> Float.of_string line | None -> 0.0
      in
      let sql =
        Printf.sprintf
          "INSERT INTO Industries (industry, risk) \n      VALUES (\"%s\", %f)"
          industry (risk /. 10.0)
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> printf "%s was succesfuly updated%!\n" industry
      | code -> print_endline code)

let insert_sector sector =
  let exists = row_exists "Sectors" "sector" sector in
  match exists with
  | true -> ()
  | false -> (
      let risk =
        printf
          "Sector doesn't exist in database\n\
           Please write estimated security of %s sector (0-10)\n\
           %!"
          sector;
        let input = In_channel.input_line In_channel.stdin in
        match input with Some line -> Float.of_string line | None -> 0.0
      in
      let sql =
        Printf.sprintf
          "INSERT INTO Sectors (sector, risk) \n      VALUES (\"%s\", %f)"
          sector (risk /. 10.0)
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> printf "%s was succesfuly updated\n%!" sector
      | code -> print_endline code)

let insert_ratings symbol base_rating target_rating =
    let exists = row_exists "Ratings" "symbol" symbol in
    match exists with
    | false ->
      let sql = Printf.sprintf
        "INSERT INTO RATINGS (symbol, base_rating, target_rating)
         VALUES (\"%s\", %f, %f)" symbol base_rating target_rating
      in
        let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
        (match inserted with
        | "OK" -> ()
        | code ->
          printf "%s error: " symbol;
          print_endline code)
    | true -> 
      let sql = Printf.sprintf
         "UPDATE Ratings
          SET base_rating = %f, target_rating = %f \n     
          WHERE symbol =  \"%s\"" base_rating target_rating symbol
      in
        let updated = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
        (match updated with
        | "OK" -> ()
        | code ->
          printf "%s error: " symbol;
          print_endline code)
  
let insert_country country =
  let exists = row_exists "Countries" "country" country in
  match exists with
  | true -> ()
  | false -> (
      let tax =
        printf
          "Country doesn't exist in database\n\
           Please write tax rate %s (0-100%%)\n\
           %!"
          country;
        let input = In_channel.input_line In_channel.stdin in
        match input with Some line -> Float.of_string line | None -> 0.0
      in
      let bond_rate =
        printf "Please write 10y bond rate of %s (0-100%%)\n%!" country;
        let input = In_channel.input_line In_channel.stdin in
        match input with Some line -> Float.of_string line | None -> 0.0
      in
      let sql =
        Printf.sprintf
          "INSERT INTO Countries (country, tax, bonds_rate) \n\
          \      VALUES (\"%s\", %f, %f)" country (tax /. 100.0)
          (bond_rate /. 100.0)
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> printf "%s was succesfuly updated\n" country
      | code -> print_endline code)

let update_stock ticker
    (price, beta, market_cap, currency, industry, sector, country, pullable) =
  let stock_exists = row_exists "Stocks" "symbol" ticker in
  match stock_exists with
  | false -> (
      insert_country country;
      insert_sector sector;
      insert_industry industry;
      let sql =
        Printf.sprintf
          "INSERT INTO Stocks (symbol, price, beta, market_cap, currency, \n\
          \                          industry, sector, country, pullable)\n\
          \      VALUES (\"%s\",%f,%f,%i,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\");"
          ticker price beta market_cap currency industry sector country pullable
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> printf "%s was succesfuly pulled\n" ticker
      | code -> 
      print_endline sql;
        print_endline code)
  | true -> (
      let sql =
        Printf.sprintf
          "UPDATE Stocks Set\n\
          \        price = %f,\n\
          \        beta = %f,\n\
          \        market_cap = %i,\n\
          \        currency = \"%s\", \n\
          \        industry = \"%s\",\n\
          \        sector = \"%s\",\n\
          \        country = \"%s\",\n\
          \        pullable = \"%s\"\n\
          \ Where symbol = \"%s\";" price beta market_cap currency industry
          sector country pullable ticker
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> printf "%s was succesfuly updated\n%!" ticker
      | code ->
         print_endline code)

let delete_financials ticker =
  let sql =
    Printf.sprintf "DELETE FROM Financials where symbol = \"%s\"" ticker
  in
  let deleted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match deleted with "OK" -> true | _ -> false

let update_financials ticker
    ( year,
      period,
      revenue,
      net_income,
      cash,
      assests,
      debt,
      free_cash_flow,
      currency ) =
  let sql =
    Printf.sprintf
      "INSERT INTO Financials (symbol, year, period, net_income, free_cash_flow,\n\
      \                             revenue, cash, \
       total_assests, total_debt, currency)\n\
      \      VALUES (\"%s\",\"%s\",\"%s\",%i, %i, %i, %i, %i, %i, \
       \"%s\");"
      ticker year period net_income free_cash_flow revenue cash assests
      debt currency
  in
  let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match inserted with
  | "OK" -> ()
  | code -> print_endline code

let update_price ticker (price, market_cap, pe) =
  let stock_exists = row_exists "Stocks" "symbol" ticker in
  match stock_exists with
  | false -> ()
  | true -> (
      let sql =
        Printf.sprintf
          "UPDATE Stocks Set\n\
          \        price = %f,\n\
          \        market_cap = %i,\n\
          \        pe = %f\n\
          \ \n\
          \         Where symbol = \"%s\";" price market_cap pe ticker
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> printf "%s price was succesfuly updated\n%!" ticker
      | code -> print_endline code)

let select_ratings_data () =
  let sql =
    Printf.sprintf "
    SELECT
      s.symbol as ss, s.industry, s.sector, s.market_cap * s.ratio,
      s.pe, s.price * s.ratio, f.total_debt * f.ratio,
      c.tax, c.bonds_rate, s.status, s.target, MAX(f.year) as ye
    FROM (
		SELECT Stocks.*, CASE WHEN Stocks.currency = \"EUR\" OR Currencies.ratio IS NULL THEN 1.0 ELSE Currencies.ratio END AS ratio
		FROM Stocks
		LEFT JOIN Currencies
		ON Stocks.currency = Currencies.currency ) 
		AS s
    LEFT JOIN (
		SELECT Financials.*, CASE WHEN Financials.currency = \"EUR\" OR Currencies.ratio IS NULL THEN 1.0 ELSE Currencies.ratio END AS ratio
		FROM Financials 
		LEFT JOIN Currencies
		ON Financials.currency = Currencies.currency) 
		AS f
    ON f.symbol = s.symbol
    LEFT JOIN Countries AS c
    on s.country = c.country
    GROUP BY s.symbol
    ORDER BY s.symbol ASC, f.year DESC;
    "
  in
  let stmt = Sqlite3.prepare db sql in
  let sql_data = fetch_results stmt in
  let rec results data =
    match data with
    | hd :: tl -> (
        match hd with
        | tick_symbol :: industry :: sector :: market_cap :: pe :: price
          :: debt :: tax 
          :: bond_rate :: status :: target :: _ ->
            [
              ( Sqlite3.Data.to_string_exn tick_symbol,
                Sqlite3.Data.to_string_exn industry,
                Sqlite3.Data.to_string_exn sector,
                Sqlite3.Data.to_float_exn market_cap,
                Sqlite3.Data.to_float_exn pe,
                Sqlite3.Data.to_float_exn price,
                Sqlite3.Data.to_float_exn debt,
                Sqlite3.Data.to_float_exn tax,
                Sqlite3.Data.to_float_exn bond_rate, 
                Sqlite3.Data.to_string_exn status,
                Sqlite3.Data.to_int_exn target
                );
            ]
            @ results tl
        | _ -> [])
    | [] -> []
  in
  results sql_data

let update_speculations symbol growth return moat =
  let sql_delete =
    Printf.sprintf
      "\n    DELETE FROM Speculative_data WHERE symbol = \"%s\"\n    " symbol
  in
  let deleted = Sqlite3.exec db sql_delete |> Sqlite3.Rc.to_string in
  match deleted with
  | "OK" -> (
      let sql =
        Printf.sprintf
          "\n\
          \      INSERT INTO Speculative_data (symbol, growth, \
           required_return, moat)\n\
          \      VALUES (\"%s\", %f, %f, %f); \n\
          \      " symbol growth return moat
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> print_endline "Speculation updated"
      | code -> printf "Update failed: error %s" code)
  | code -> print_endline code

let insert_forex names prices =
  let rec f names prices =
    match names with
    | hd1 :: tl1 -> (
        match prices with
        | hd2 :: tl2 ->
          Printf.sprintf "(\"%s\", \"%s\", %f), " hd1 "EUR" hd2 ^ f tl1 tl2
        | [] -> "")
    | [] -> ""
  in
  let deleted =
    Sqlite3.exec db "DELETE FROM Currencies" |> Sqlite3.Rc.to_string
  in
  match deleted with
  | "OK" -> (
      let unclean_values = f names prices in
      let values_string =
        String.sub unclean_values ~pos:0 ~len:(String.length unclean_values - 2)
      in
      let sql =
        Printf.sprintf
          "\n\
          \        INSERT INTO Currencies (currency, target, ratio)\n\
          \        VALUES %s" values_string
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> print_endline "Forex updated"
      | code -> failwith code)
  | code -> failwith code

let select_currencies () =
  let sql = "
    SELECT s.currency
    FROM (SELECT DISTINCT currency FROM Stocks) as s
    WHERE s.currency != \"EUR\"
    UNION 
    SELECT f.currency
    FROM (SELECT DISTINCT currency FROM Financials) as f
    WHERE f.currency != \"EUR\"
    "
  in  
  let stmt = Sqlite3.prepare db sql in
  let data = fetch_results stmt in
  List.map data ~f:(fun x ->
    List.map x ~f:(fun y -> Sqlite3.Data.to_string_exn y)
    |> String.concat)

let select_first_and_last_fcf () =
  let cashflow_window_10y = "
    WITH T AS (
      SELECT RANK() OVER (ORDER BY substr(f.year, 1, 4), symbol) Rank,
        substr(f.year, 1, 4) as year, f.symbol, f.free_cash_flow
      FROM Financials f
    ),
    sum_of_quarters AS (
    	SELECT DISTINCT *
    	FROM (SELECT symbol, year, sum(free_cash_flow) as free_cash_flow
    	FROM T
    	GROUP BY year, symbol
    	HAVING count(rank) = 4
    	UNION
    	SELECT symbol, year, sum(free_cash_flow) as free_cash_flow
    	FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY year DESC) AS n
    		FROM T
    	)
    	WHERE n <= 4
    	Group by symbol)
    ),
    temp_table AS (
    SELECT f.symbol, f.year, f.free_cash_flow, n
    	FROM (
    		SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY year DESC) AS n
    		FROM sum_of_quarters
    	) AS f
    	WHERE n <= 10 
    )
       "
  in  
  let n = [3; 5; 6; 8; 10] in
  let n_year_max_min = List.map n ~f:(fun x ->  
  Printf.sprintf " 
		SELECT a.symbol, a.free_cash_flow, b.free_cash_flow, b.n
        FROM (
        	SELECT symbol, free_cash_flow, n, max(year)
        	FROM 
        	temp_table 
			WHERE n <= %d
        	Group by symbol) a
        LEFT JOIN (
        	SELECT symbol, free_cash_flow, n, min(year) 
        	FROM 
        	temp_table
			WHERE n <= %d
        	Group by symbol) b 
      	ON b.symbol = a.symbol
      	" x x ^ (if not (Int.(=) x 10) then "Union" else ""))
  in
  let sql = cashflow_window_10y ^ String.concat n_year_max_min  ^  "ORDER BY a.symbol ASC, b.n ASC;" in
  let stmt = Sqlite3.prepare db sql in
  let data = fetch_results stmt in
  let rec results data =
    match data with
    | hd :: tl -> (
        match hd with
        | tick_symbol :: new_free_cash_flow :: old_free_cash_flow :: duration :: _ ->
            [
              ( Sqlite3.Data.to_string_exn tick_symbol,
                Sqlite3.Data.to_int_exn new_free_cash_flow |> Float.of_int,
                Sqlite3.Data.to_int_exn old_free_cash_flow |> Float.of_int,
                Sqlite3.Data.to_int_exn duration |> Float.of_int
                (* TODO duration set to max of n if duration is maller *)
                );
            ]
            @ results tl
        | _ -> [])
    | [] -> []
  in
  results data

let delete_stock ticker_symbol =
  let sql = Printf.sprintf "
    DELETE FROM Stocks WHERE symbol = \"%s\";
    DELETE FROM Financials WHERE symbol = \"%s\";
    DELETE FROM Speculative_data WHERE symbol = \"%s\";
    " ticker_symbol ticker_symbol ticker_symbol
  in
  let deleted =
    Sqlite3.exec db sql |> Sqlite3.Rc.to_string
  in
  match deleted with
  | "OK" -> printf "%s removed\n" ticker_symbol
  | code -> printf "Error: %s\n" code
