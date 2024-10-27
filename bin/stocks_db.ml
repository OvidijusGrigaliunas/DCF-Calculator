open Base
open Stdio

let db = Secrets.current_dir ^ "stocks" |> Sqlite3.db_open

let rec print_results a =
  match a with
  | [] -> ()
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
    (price, beta, div_yield, market_cap, currency, industry, sector, country, pullable) =
  let stock_exists = row_exists "Stocks" "symbol" ticker in
  match stock_exists with
  | false -> (
      insert_country country;
      insert_sector sector;
      insert_industry industry;
      let sql =
        Printf.sprintf
          "INSERT INTO Stocks (symbol, price, beta, div_yield, market_cap, currency, \n\
          \                          industry, sector, country, pullable)\n\
          \      VALUES (\"%s\",%f,%f, %f,%i,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\");"
          ticker price beta div_yield market_cap currency industry sector country pullable
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> ()
      | code -> 
        print_endline code)
  | true -> (
      let sql =
        Printf.sprintf
          "UPDATE Stocks Set\n\
          \        price = %f,\n\
          \        beta = %f,\n\
          \        div_yield = %f,\n\
          \        market_cap = %i,\n\
          \        currency = \"%s\", \n\
          \        industry = \"%s\",\n\
          \        sector = \"%s\",\n\
          \        country = \"%s\",\n\
          \        pullable = \"%s\"\n\
          \ Where symbol = \"%s\";" price beta div_yield market_cap currency industry
          sector country pullable ticker
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> ()
      | code ->
         print_endline code)

let delete_financials ticker =
  let sql =
    Printf.sprintf "DELETE FROM Financials where symbol = \"%s\"" ticker
  in
  let deleted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match deleted with "OK" -> true | _ -> false

let update_financials ticker financials =
  let rec gen_sql financials =
    match financials with
    | (year, period, net_income, free_cash_flow, revenue, cash, assests, debt, currency) :: tl ->
      let sql_values = Printf.sprintf "
        (\"%s\",\"%s\",\"%s\",%i, %i, %i, %i, %i, %i, \"%s\") "
        ticker year period net_income free_cash_flow revenue cash assests debt currency
      in     
      let line =
        match tl with
        | [] -> sql_values
        | _ -> sql_values ^ ","
      in 
      String.append line (gen_sql tl)
       
    | [] -> ""
  in
  let base_sql = 
     "INSERT INTO Financials (symbol, year, period, net_income, free_cash_flow,\n\
      \                             revenue, cash, \
       total_assests, total_debt, currency)\n
       VALUES 
       "
  in
  let sql = base_sql ^ gen_sql financials in
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
      | "OK" -> ()
      | code -> print_endline code)

let select_ratings_data () =
  let sql =
    Printf.sprintf "
    SELECT
      s.symbol as ss, s.industry, s.sector, s.market_cap * s.ratio,
      s.pe, s.price * s.ratio, f.total_debt * f.ratio,
      c.tax, c.bonds_rate, s.status, s.target, s.div_yield, MAX(f.year) as ye
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
  		ON Financials.currency = Currencies.currency
  		WHERE period = \"FQ\") 
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
          :: bond_rate :: status :: target :: div_yield :: _ ->
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
                Sqlite3.Data.to_int_exn target,
                Sqlite3.Data.to_float_exn div_yield
                );
            ]
            @ results tl
        | _ -> [])
    | [] -> []
  in
  results sql_data

let delete_earnings ticker =
  let sql =
    Printf.sprintf "DELETE FROM Earnings WHERE symbol = \"%s\"" ticker
  in
  let deleted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match deleted with "OK" -> () | code -> print_endline code

let update_earnings ticker earnings =
  delete_earnings ticker;
  let rec gen_sql earnings =
    match earnings with
    | (year, period, eps) :: tl ->
      let sql_values = Printf.sprintf "
        (\"%s\",\"%s\", \"%s\",%f)"
        ticker year period eps
      in     
      let line =
        match tl with
        | [] -> sql_values
        | _ -> sql_values ^ ","
      in 
      String.append line (gen_sql tl)
       
    | [] -> ""
  in
  let base_sql = 
     "INSERT INTO Earnings (symbol, year, period, eps)\n
       VALUES 
       "
  in
  let sql = base_sql ^ gen_sql earnings in
  let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match inserted with
  | "OK" -> ()
  | code -> print_endline code

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

let clean_up_financials_currency () =
  let sql = "
    WITH cur AS (
    	Select symbol, currency from Financials where symbol IN
    		(Select symbol from Financials where currency = \"None\") 
    		AND NOT currency =\"None\"
    	GROUP by symbol
    )
    UPDATE Financials
    SET currency = cur.currency
    FROM cur
    WHERE Financials.symbol = cur.symbol
    "
  in
  let cleaned_up = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match cleaned_up with
  | "OK" -> ()
  | code -> printf "Failed forex cleanup: error %s" code

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
      let cashflow_window_10y = 
      "WITH A AS (
       SELECT Rank, f.year, f.symbol, CAST(f.free_cash_flow * 1.0 * ratio AS INT) as free_cash_flow, min_year
        FROM (
      	SELECT RANK() OVER (ORDER BY substr(f.year, 1, 4), symbol) Rank,
          substr(f.year, 1, 4) as year, 
      	f.symbol, 
      	f.free_cash_flow, 
      	min(substr(f.year, 1, 4)) over (PARTITION BY f.symbol) as min_year
      	FROM Financials f
      	WHERE period = \"FQ\") f
        LEFT JOIN (
      	SELECT DISTINCT Financials.symbol, CASE WHEN Financials.currency = \"EUR\" OR Currencies.ratio IS NULL THEN 1.0 ELSE Currencies.ratio END AS ratio
      	FROM Financials 
      	LEFT JOIN Currencies
      	ON Financials.currency = Currencies.currency
      	WHERE period = \"FQ\") b
      	ON f.symbol = b.symbol
        WHERE not min_year = year
      ),
      count_reports_per_year AS (
      	SELECT symbol, year, COUNT(Rank) as reports, 
      	LAG(COUNT(Rank)) OVER (PARTITION BY symbol ORDER BY symbol, year) as prev_year_report_n
      	FROM A
      	GROUP BY year, symbol
      ),
      T AS (
        SELECT A.*, c.reports, c.prev_year_report_n
        FROM A
        INNER JOIN count_reports_per_year c
      	on A.year = c.year AND A.symbol = c.symbol
      ),
      sum_of_q AS (
      	SELECT symbol, year, free_cash_flow
      	FROM (
      		SELECT symbol, substr(f.year, 1, 4) as year, sum(free_cash_flow) as free_cash_flow
      		FROM Financials f
      		WHERE period = \"FY\"
      		GROUP BY year, symbol
      		ORDER BY symbol, year DESC)
      	UNION
      	SELECT symbol, year, sum(free_cash_flow) as free_cash_flow
      	FROM (
      		SELECT l.year, l.symbol, l.free_cash_flow, k.prev_year_report_n, ROW_NUMBER() OVER (PARTITION BY l.symbol ORDER BY l.year DESC) AS n
      			FROM T as l
      		LEFT JOIN (
      			SELECT max(year) as year, symbol, prev_year_report_n
      			FROM T
      			GROUP BY Symbol) as k
      		ON k.symbol = l.symbol 
      	)
      	WHERE n <= prev_year_report_n
      	Group by symbol
      	ORDER BY symbol, year DESC
	
      ),  
      temp_table AS (
      SELECT f.symbol, f.year, f.free_cash_flow, n
      	FROM (
      		SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY year DESC) AS n
      		FROM sum_of_q
      	) AS f
      )
    "
  in  
  let n = [2; 3; 4; 5; 6] in
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
      	" x x ^ (if not (Int.(=) x 6) then "Union" else ""))
  in
  let sql = cashflow_window_10y ^ String.concat n_year_max_min  ^  " ORDER BY a.symbol ASC, b.n ASC;" in
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
    DELETE FROM Ratings WHERE symbol = \"%s\";
    DELETE FROM Speculative_data WHERE symbol = \"%s\";
    " ticker_symbol ticker_symbol ticker_symbol ticker_symbol
  in
  let deleted =
    Sqlite3.exec db sql |> Sqlite3.Rc.to_string
  in
  match deleted with
  | "OK" -> printf "%s removed\n" ticker_symbol
  | code -> printf "Error: %s\n" code

let select_eps_growth () =
    let eps_window = "
      WITH A AS (
       SELECT Rank, f.year, f.full_date, f.symbol, f.eps, min_year
        FROM (
      	SELECT RANK() OVER (ORDER BY substr(f.year, 1, 4), symbol) Rank,
          substr(f.year, 1, 4) as year, 
      	f.year as full_date,
      	f.symbol, 
      	f.eps, 
      	min(substr(f.year, 1, 4)) over (PARTITION BY f.symbol) as min_year
      	FROM Earnings f
      	WHERE period = \"FQ\"
      	ORDER BY f.year) f
        WHERE not min_year = year and year > \"2014\"
      ),
      count_reports_per_year AS (
      	SELECT symbol, year, COUNT(Rank) as reports, 
      	LAG(COUNT(Rank)) OVER (PARTITION BY symbol ORDER BY symbol, year) as prev_year_report_n
      	FROM A
      	GROUP BY year, symbol
      ),
      T AS (
        SELECT A.*, c.reports, c.prev_year_report_n
        FROM A
        INNER JOIN count_reports_per_year c
      	on A.year = c.year AND A.symbol = c.symbol
      ),
      sum_of_q AS (
      	SELECT symbol, year, eps
      	FROM (
      		SELECT f.symbol, substr(f.year, 1, 4) as year, f.eps 
      		FROM Earnings f
      		WHERE period = \"FY\" AND year > \"2014\"
      		ORDER BY f.symbol, year DESC)
      	UNION
      	SELECT symbol, year, sum(eps) as eps
      	FROM (
				
      		SELECT l.year, l.full_date, l.symbol, l.eps, k.prev_year_report_n, ROW_NUMBER() OVER (PARTITION BY l.symbol ORDER BY l.year DESC) AS n
      			FROM T as l
      		LEFT JOIN (
      			SELECT max(year) as year, symbol, prev_year_report_n
      			FROM T
      			GROUP BY Symbol) as k
      		ON k.symbol = l.symbol 
      		ORDER BY l.symbol, full_date desc
      	)
      	WHERE n <= prev_year_report_n
      	Group by symbol
      	ORDER BY symbol, year DESC
	
      ),  
      latest_eps_check AS (
      SELECT symbol, year, eps
      FROM ( 
      	SELECT *, MAX(eps) OVER (PARTITION BY symbol, year) AS max_eps, 
      	lag(eps) OVER (PARTITION BY symbol ORDER BY symbol, year) AS lag_eps,
      	lag(year) OVER (PARTITION BY symbol ORDER BY symbol, year) AS lag_year
      	FROM sum_of_q) as f
      	WHERE eps = max_eps AND NOT (lag_eps = eps AND lag_year = year)
       ),
      temp_table AS (
      SELECT f.symbol, f.year, f.eps, n
      	FROM (
      		SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY year DESC) AS n
      		FROM latest_eps_check
      	) AS f
      )
    "
  in  
  let n = [2; 3; 4; 5; 6; 7] in
  let n_year_growth = List.map n ~f:(fun x ->  
  Printf.sprintf " 
  SELECT a.symbol, 
  	CASE 
  	WHEN NOT (pow((a.eps - b.eps)/abs(B.eps)+1.0, 1.0 / b.n) - 1.0) IS NULL 
  		THEN pow((a.eps - b.eps)/abs(B.eps)+1.0, 1.0 / b.n) - 1.0
  	ELSE 0.0
  	END,
  	 b.n
          FROM (
          	SELECT symbol, eps, n, max(year)
          	FROM 
          	temp_table 
  			WHERE n <= %d
          	Group by symbol) a
          LEFT JOIN (
          	SELECT symbol, eps, n, min(year) 
          	FROM 
          	temp_table
  			WHERE n <= %d
          	Group by symbol) b 
        	ON b.symbol = a.symbol
   	" x x ^ (if not (Int.(=) x 7) then "Union" else ""))
  in
  let sql = eps_window ^ String.concat n_year_growth ^ "ORDER BY a.symbol ASC, b.n ASC;" in
  let stmt = Sqlite3.prepare db sql in
  let data = fetch_results stmt in
  let rec results data =
    match data with
    | hd :: tl -> (
        match hd with
        | tick_symbol :: eps_growth :: duration :: _ ->
            [
              ( Sqlite3.Data.to_string_exn tick_symbol,
                Sqlite3.Data.to_float_exn eps_growth,
                Sqlite3.Data.to_int_exn duration |> Float.of_int
                );
            ]
            @ results tl
        | _ -> [])
    | [] -> []
  in
  results data

