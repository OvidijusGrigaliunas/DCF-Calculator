open Base
open Stdio

type t = {
  symbol: string;
  rating: float;
  date: string;
  yearly_return: float;
  range: (float * float);
}

let get_range value range_gap =
  let r_start =
    value -. (Float.mod_float value range_gap) 
  in
  let r_end = r_start +. range_gap in
  (r_start, r_end)

let range_equal range1 range2 =
  let start1, end1 = range1 in
  let start2, end2 = range2 in
  let is_equal1 = Float.(=) start1 start2 in
  let is_equal2 = Float.(=) end1 end2 in
  (is_equal1 && is_equal2) 

let fetch_backtest_data () =
  let sql =
    Printf.sprintf
    "
      SELECT
      	symbol
      	, rating
      	, \"date\"
      	, y_return
      FROM backtest_ratings_returns
  	  WHERE rating BETWEEN -5 AND 10
      ORDER BY symbol, rating;
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
        |	symbol :: rating :: date :: y_return :: _ ->
          let rating = Sqlite3.Data.to_float_exn rating in
          let range = get_range rating 0.10 in
          let data : t =
            {
              symbol = Sqlite3.Data.to_string_exn symbol;
              rating = rating;
              date = Sqlite3.Data.to_string_exn date;
              yearly_return = Sqlite3.Data.to_float_exn y_return;
              range = range
             } 
          in
          f (data :: accum)
        | _ -> f accum
      )
    | Rc.ERROR -> failwith (Sqlite3.Rc.to_string Rc.ERROR)
    | _ -> failwith "Unexpected result code"
  in
  f []

let calc_rating_risk rating =
  if (Float.(>) rating 1.0) then
    1.0 +. Float.log(rating) /. (2.0 +. Float.abs(Float.log(rating)))
  else
    1.0 -. (1.0 -. rating) /. (15.0 +. (1.0 -. rating))

let calc_avg_returns data =
  let rec while_range range stock_data =  
    match stock_data with
    | [] -> ([], stock_data)
    | (hd : t) :: tl
      when range_equal range hd.range ->
      let same, rest = while_range range tl in
      (hd :: same, rest)
    | _  -> ([], stock_data)
  in
  let rec loop stock_data =
    match stock_data with
    | [] -> []
    | (hd : t) :: tl -> 
      let same_range, rest_of_list = while_range hd.range tl in
      let data_set : (t list) = hd :: same_range in
      let sum = List.fold data_set ~init:0.0 ~f:(
        fun acc (x : t) ->
          let risk_averse_return = 
            if (Float.(<) x.yearly_return 0.0) then x.yearly_return *. 1.5 *. calc_rating_risk(x.rating) else x.yearly_return
          in 
          acc +. risk_averse_return
        )
      in
      let avg = sum /. (List.length data_set |> Float.of_int) in
      [(hd.range, avg)] @ loop rest_of_list
  in
  loop data    
  
let returns_to_sql_values symbol returns_range =
  let avg_return, r_start, r_end = returns_range in 
  let sql_values = Printf.sprintf "
     (\"%s\", %f, %f, %f),"
    symbol avg_return r_start r_end
  in   
  sql_values
    
let insert_returns results_sql  =
  let base_sql =  
     "INSERT INTO Returns_of_ratings (symbol, return, r_start, r_end)\n
       VALUES 
       " 
  in
  let sql = base_sql ^ results_sql in
  let inserted = Sqlite3.exec Stocks_db.db sql |> Sqlite3.Rc.to_string in
  match inserted with
  | "OK" ->  ();
  | _ -> print_endline "Couldn't insert return values "

let delete_ratings () =
  let sql =
    Printf.sprintf "DELETE FROM Returns_of_ratings" 
  in
  let deleted = Sqlite3.exec Stocks_db.db sql |> Sqlite3.Rc.to_string in
  match deleted with
  | "OK" -> ()
  | code -> printf "Error deleting from Returns_of_ratings: %s\n%!" code

let calc_possible_returns data =
  let data_length = List.length data |> Float.of_int in
  let data_left = ref data_length in
  let sql_buffer = Buffer.create 10000 in 
  let rec while_symbol symbol stock_data =  
    match stock_data with
    | [] -> ([], stock_data)
    | (hd : t) :: tl
      when String.(=) hd.symbol symbol ->
      let same, rest = while_symbol symbol tl in
      (hd :: same, rest)
    | _  -> ([], stock_data)
  in
  let rec loop stock_data =
    match stock_data with
    | [] -> ()
    | (hd : t) :: tl -> 
      let same_symbols, rest_of_list = while_symbol hd.symbol tl in
      let data_set : (t list) = hd :: same_symbols in
      let data_set_length = List.length data_set |> Float.of_int in
      let avg_returns = calc_avg_returns data_set in

      data_left := !data_left -. data_set_length; 
      List.iter avg_returns ~f:(
        fun ((b, c), a) ->
        Buffer.add_string sql_buffer (returns_to_sql_values hd.symbol (a, b, c))
      );
      let len = Buffer.length sql_buffer in
      Stdlib.Buffer.truncate sql_buffer (len - 1);
      let sql = Buffer.contents sql_buffer in
      insert_returns sql; 
      Buffer.clear sql_buffer;
      printf "\r%.1f%% %!        " (100.0 -. !data_left /. data_length *. 100.0); 
      loop rest_of_list
  in
  printf "\r%.1f%% %!        " (0.0); 
  loop data       
  

let backtest () =
  delete_ratings ();
  let start_time = Unix.gettimeofday () in
  print_endline "Extracting data";
  let data = fetch_backtest_data () in
  printf "\rData extracted in %.0f seconds\n%!" (Unix.gettimeofday () -. start_time);

  let start_time = Unix.gettimeofday () in
  calc_possible_returns data;
  printf "\rPossible return calculations are completed in %.0f seconds\n%!" (Unix.gettimeofday () -. start_time)
 
