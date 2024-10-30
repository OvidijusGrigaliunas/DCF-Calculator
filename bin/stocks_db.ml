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
    (beta, div_yield, shares, currency, industry, sector, country, pullable) =
  let stock_exists = row_exists "Stocks" "symbol" ticker in
  match stock_exists with
  | false -> (
      insert_country country;
      insert_sector sector;
      insert_industry industry;
      let sql =
        Printf.sprintf
          "INSERT INTO Stocks (symbol, beta, div_yield, shares, currency, \n\
          \                          industry, sector, country, pullable)\n\
          \      VALUES (\"%s\",%f, %f,%i,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\");"
          ticker beta div_yield shares currency industry sector country pullable
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
          \        beta = %f,\n\
          \        div_yield = %f,\n\
          \        shares = %i,\n\
          \        currency = \"%s\", \n\
          \        industry = \"%s\",\n\
          \        sector = \"%s\",\n\
          \        country = \"%s\",\n\
          \        pullable = \"%s\"\n\
          \ Where symbol = \"%s\";" beta div_yield shares currency industry
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
  | "OK" ->  clean_up_financials_currency ();
  | code -> print_endline code

  
let update_pe_ttm ticker =
  let stock_exists = row_exists "Stocks" "symbol" ticker in
  match stock_exists with
  | false -> ()
  | true -> (
      let sql =
        Printf.sprintf "
      WITH fin_ratios as (
      	SELECT DISTINCT Financials.symbol, CASE WHEN Financials.currency = \"EUR\" OR Currencies.ratio IS NULL THEN 1.0 ELSE Currencies.ratio END AS ratio
      	FROM Financials 
      		LEFT JOIN Currencies
      	ON Financials.currency = Currencies.currency
      ),
      price_ratios as (
      	SELECT s.symbol, CASE WHEN s.currency = \"EUR\" OR c.ratio IS NULL THEN 1.0 ELSE c.ratio END AS ratio
      	FROM STOCKS s 
      		LEFT JOIN Currencies c
      	ON s.currency = c.currency
      )
      UPDATE Stocks
      SET pe = (
      SELECT (s.price * pr.ratio)/ (SUM(et.net_income * fr.ratio) / s.shares)
      FROM (
      	SELECT 
      		symbol
      		, net_income
      		, year
      	FROM Financials e
      		WHERE e.symbol = STOCKS.symbol AND period = \"FQ\"
      	ORDER BY year DESC
      	LIMIT 4) AS et
      LEFT JOIN Stocks s
      		ON et.symbol = s.symbol
      LEFT JOIN fin_ratios fr
      	ON s.symbol = fr.symbol
      LEFT JOIN price_ratios pr
      	ON s.symbol = pr.symbol
      GROUP BY s.symbol)
      WHERE Stocks.symbol = \"%s\"
        " ticker
      in
      let updated = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match updated with
      | "OK" -> ()
      | code -> print_endline code)

let update_price ticker price =
  let stock_exists = row_exists "Stocks" "symbol" ticker in
  match stock_exists with
  | false -> ()
  | true -> (
      let sql =
        Printf.sprintf
          "UPDATE Stocks Set\n\
          \        price = %f\n\
          \ \n\
          \         Where symbol = \"%s\";" price ticker
      in
      let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
      match inserted with
      | "OK" -> update_pe_ttm ticker
      | code -> print_endline code)

let select_ratings_data () =
  let sql =
    Printf.sprintf "
    SELECT
      s.symbol as ss, s.industry, s.sector, s.shares * 1.0,
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
        | tick_symbol :: industry :: sector :: shares :: pe :: price
          :: debt :: tax 
          :: bond_rate :: status :: target :: div_yield :: _ ->
          (try 
            [
              ( Sqlite3.Data.to_string_exn tick_symbol,
                Sqlite3.Data.to_string_exn industry,
                Sqlite3.Data.to_string_exn sector,
                Sqlite3.Data.to_float_exn shares,
                Sqlite3.Data.to_float_exn pe,
                Sqlite3.Data.to_float_exn price,
                Sqlite3.Data.to_float_exn debt,
                Sqlite3.Data.to_float_exn tax,
                Sqlite3.Data.to_float_exn bond_rate, 
                Sqlite3.Data.to_string_exn status,
                Sqlite3.Data.to_float_exn target,
                Sqlite3.Data.to_float_exn div_yield
                );
            ]
            @ results tl
          with
          | a -> 
            let b = Exn.to_string a in
            printf "%s has bad data\n" (Sqlite3.Data.to_string_exn tick_symbol);
            printf "Error : %s\n" b;
            [] @ results tl)
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

let delete_dividends ticker =
  let sql =
    Printf.sprintf "DELETE FROM Dividends WHERE symbol = \"%s\"" ticker
  in
  let deleted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match deleted with "OK" -> () | code -> print_endline code

let update_dividends ticker dividends =
  delete_dividends ticker;
  let rec gen_sql dividends =
    match dividends with
    | (year, amount) :: tl ->
      let sql_values = Printf.sprintf "
        (\"%s\",\"%s\", %f)"
        ticker year amount
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
     "INSERT INTO Dividends (symbol, ex_date, amount)\n
       VALUES 
       "
  in
  let sql = base_sql ^ gen_sql dividends in
  let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match inserted with
  | "OK" -> ()
  | code -> print_endline code

let delete_splits ticker =
  let sql =
    Printf.sprintf "DELETE FROM Splits WHERE symbol = \"%s\"" ticker
  in
  let deleted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match deleted with "OK" -> () | code -> print_endline code
let clean_splits () =
  let sql = "DELETE FROM Splits WHERE \"date\" = \"\" OR amount = 0.0" in
  let clean= Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match clean with "OK" -> () | code -> print_endline code

let update_splits ticker splits =
  delete_splits ticker;
  let rec gen_sql splits =
    match splits with
    | (year, amount) :: tl ->
      let sql_values = Printf.sprintf "
        (\"%s\",\"%s\", %f)"
        ticker year amount
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
     "INSERT INTO splits (symbol, date, amount)\n
       VALUES 
       "
  in
  let sql = base_sql ^ gen_sql splits in
  let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match inserted with
  | "OK" -> ()
  | code -> print_endline code

let delete_history_prices ticker =
  let sql =
    Printf.sprintf "DELETE FROM Historical_prices WHERE symbol = \"%s\"" ticker
  in
  let deleted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match deleted with "OK" -> () | code -> print_endline code

let update_history_prices ticker prices =
  delete_history_prices ticker;
  let rec gen_sql prices =
    match prices with
    | (year, price) :: tl ->
      let sql_values = Printf.sprintf "
        (\"%s\",\"%s\", %f)"
        ticker year price
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
     "INSERT INTO Historical_prices (symbol, date, price)\n
       VALUES 
       "
  in
  let sql = base_sql ^ gen_sql prices in
  let inserted = Sqlite3.exec db sql |> Sqlite3.Rc.to_string in
  match inserted with
  | "OK" -> ()
  | code -> print_endline ("History data update error:" ^ code)

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
  let temp_table = 
    "
    DROP TABLE IF EXISTS Temp_dcf;
    CREATE TEMPORARY TABLE Temp_dcf AS
    SELECT *
    FROM DCF_data;
    "
      
  in  
  let n = [2; 3; 4; 5; 6; 7; 8; 9; 10] in
  let n_year_max_min = List.map n ~f:(fun x ->  
  Printf.sprintf "  
  	SELECT a.symbol, a.free_cash_flow, b.free_cash_flow, b.n
        FROM (
        	SELECT symbol, free_cash_flow, n, max(year)
        	FROM 
        	Temp_dcf
        	Group by symbol) a
        LEFT JOIN (
        	SELECT symbol, free_cash_flow, n
        	FROM 
        	Temp_dcf
    			WHERE n = %d
        	Group by symbol) b 
      	ON b.symbol = a.symbol
		WHERE NOT (b.free_cash_flow IS NULL OR b.n IS NULL) 

      	" x ^ (if not (Int.(=) x 10) then "Union" else ""))
  in
  let _ = Sqlite3.exec db temp_table in
  let sql = String.concat n_year_max_min  ^  " ORDER BY a.symbol ASC, b.n ASC;" in
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
    DELETE FROM Financials WHERE symbol = \"%s\";
    DELETE FROM Ratings WHERE symbol = \"%s\";
    DELETE FROM Speculative_data WHERE symbol = \"%s\";
    DELETE FROM Earnings WHERE symbol = \"%s\";
    DELETE FROM Dividends WHERE symbol = \"%s\";
    DELETE FROM Splits WHERE symbol = \"%s\";
    DELETE FROM Historical_prices WHERE symbol = \"%s\";
    DELETE FROM Stocks WHERE symbol = \"%s\";
    " ticker_symbol ticker_symbol ticker_symbol ticker_symbol ticker_symbol ticker_symbol ticker_symbol ticker_symbol
  in
  let deleted =
    Sqlite3.exec db sql |> Sqlite3.Rc.to_string
  in
  match deleted with
  | "OK" -> printf "%s removed\n" ticker_symbol
  | code -> printf "Error: %s\n" code

let select_eps_growth () =
  (* improves performace 30x *)
  let temp_table = "
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS Temp_lynch;
    CREATE TEMPORARY TABLE Temp_Lynch AS
    SELECT symbol, eps, n, year
    FROM Peter_Lynch_data; 
  "
  in
  let n = [2; 3; 4; 5; 6; 7; 8; 9; 10] in
  let n_year_growth = List.map n ~f:(fun x ->  
  Printf.sprintf " 
  SELECT a.symbol, 
  	CASE 
  	WHEN NOT (pow((a.eps - b.eps)/abs(b.eps)+1.0, 1.0 / b.n) - 1.0) IS NULL 
  		THEN pow((a.eps - b.eps)/abs(b.eps)+1.0, 1.0 / b.n) - 1.0
  	ELSE 0.0
  	END,
  	 b.n
          FROM (
          	SELECT symbol, eps, n, max(year)
          	FROM 
          	Peter_Lynch_data
          	Group by symbol) a
          LEFT JOIN (
          	SELECT symbol, eps, n 
          	FROM 
          	Peter_Lynch_data
  			WHERE n = %d
          	Group by symbol) b 
        	ON b.symbol = a.symbol
		WHERE NOT (b.eps IS NULL OR b.n IS NULL) 

    	" x ^ (if not (Int.(=) x 10) then "Union" else ""))
  in
  let _ = Sqlite3.exec db temp_table in
  let sql =  String.concat n_year_growth ^ " ORDER BY a.symbol ASC, b.n ASC;" in
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
  

let update_targets () =
  let sql = "
    WITH sec_median AS (
    	SELECT sector, AVG(base_rating) AS median
    	FROM
    	(
    	   SELECT r.symbol, base_rating, s.sector,
    		  ROW_NUMBER() OVER (Partition by s.sector ORDER BY base_rating ASC, r.symbol ASC) AS RowAsc,
    		  ROW_NUMBER() OVER (Partition by s.sector ORDER BY base_rating DESC, r.symbol DESC) AS RowDesc
    	   FROM Ratings r
    	   LEFT JOIN Stocks s
    		ON r.symbol = s.symbol
    	) data
    	WHERE
    	   RowAsc IN (RowDesc, RowDesc - 1, RowDesc + 1)
    	   GROUP BY sector
    ),
    ind_median AS (
    	SELECT industry, AVG(base_rating) AS median
    	FROM
    	(
    	   SELECT r.symbol, base_rating, s.industry,
    		  ROW_NUMBER() OVER (Partition by s.industry ORDER BY base_rating ASC, r.symbol ASC) AS RowAsc,
    		  ROW_NUMBER() OVER (Partition by s.industry ORDER BY base_rating DESC, r.symbol DESC) AS RowDesc
    	   FROM Ratings r
    	   LEFT JOIN Stocks s
    		ON r.symbol = s.symbol
    	) data
    	WHERE
    	   RowAsc IN (RowDesc, RowDesc - 1, RowDesc + 1)
    	   GROUP BY industry
    ),
    median AS (
    	SELECT  AVG(base_rating) AS median
    	FROM
    	(
    	   SELECT base_rating,
    		  ROW_NUMBER() OVER (ORDER BY base_rating ASC) AS RowAsc,
    		  ROW_NUMBER() OVER (ORDER BY base_rating DESC) AS RowDesc
    	   FROM Ratings r
    	) 
    	WHERE
    	   RowAsc IN (RowDesc, RowDesc - 1, RowDesc + 1)
    ),
    new_targets AS (
      SELECT 
      	r.symbol
      	, round(1 + sm.median * 0.08 + im.median * 0.12 - (1 - r.base_rating / m.median) * 0.6, 3) as med_target
      	, r.base_rating
      	, sm.median
      	, im.median
      	, m.median
    	FROM Ratings r
    	LEFT JOIN Stocks s
    		ON r.symbol = s.symbol
    	LEFT JOIN ind_median im
    		ON im.industry = s.industry
    	LEFT JOIN sec_median sm
    		ON sm.sector = s.sector
    	LEFT JOIN median m
    )
    UPDATE Stocks
    SET target = (
    	SELECT med_target
    	FROM new_targets
    	WHERE STOCKS.symbol = new_targets.symbol);
       "
  in
  Sqlite3.exec db sql |> Sqlite3.Rc.to_string
