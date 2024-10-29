open Base
open Stdio

let rec argument_exists available_args arg =
  match available_args with
  | [] -> false
  | hd :: _ when String.( = ) hd arg -> true
  | _ :: tl -> argument_exists tl arg

let rec find_bad_arguments available_args args =
  match args with
  | [] -> []
  | hd :: tl ->
      let rec loop available_args arg =
        match available_args with
        | hd1 :: tl1 -> (
            let is_argument_good = argument_exists hd1 arg in
            match is_argument_good with
            | true -> []
            | false -> [] @ loop tl1 arg)
        | [] -> [ arg ]
      in
      loop available_args hd @ find_bad_arguments available_args tl

let check_for_arg_dupes available_args args =
  let rec count_dupes available_args args =
    match available_args with
    | [] -> 0
    | hd :: tl ->
        let rec loop hd args =
          match args with
          | [] -> 0
          | hd1 :: tl1 -> (
              let exists = argument_exists hd hd1 in
              match exists with true -> 1 + loop hd tl1 | false -> loop hd tl1)
        in
        let used_args = loop hd args in
        if used_args > 1 then used_args + count_dupes tl args
        else count_dupes tl args
  in
  if count_dupes available_args args > 0 then true else false

let get_available_arguments command =
  match command with
  | "rate" -> [ [ "fair"; "under"; "cheap"; "low"; "owned"; "target"; "saved" ] ]
  | "update" -> [ [ "splits"; "price"; "stock"; "forex"; "targets"; "history"] ]
  | _ -> []

let check_for_bad_arguments command args =
  let available_args = get_available_arguments command in
  let bad_args = find_bad_arguments available_args args in
  let dupes_exits = check_for_arg_dupes available_args args in
  let bad_args_exists = match bad_args with [] -> false | _ -> true in
  match dupes_exits || bad_args_exists with
  | true ->
      printf "Bad \"%s\" arguments\n" command;
      true
  | false -> false

let rec print_commands commands =
  match commands with
  | [] -> printf ""
  | hd :: tl ->
      let command, arguments = hd in
      printf "%s | " command;
      printf "%s\n" arguments;
      print_commands tl

let rec find_filter default filters args =
  match args with
  | [] -> default
  | hd :: tl ->
      if List.exists filters ~f:(fun filter -> String.( = ) filter hd) then hd
      else find_filter default filters tl

let rate_command_parser args =
  let available_args = get_available_arguments "rate" in
  match available_args with
  | filters :: _ ->
      let filter = find_filter "none" filters args in
      filter
  | [] -> "none"

let update_data arg =
  match arg with
  | "price" -> Stocks_api.update_all_prices ();
    printf "Prices were succesfuly updated\n%!"
  | "stock" -> Stocks_api.update_data ();
    printf "Stocks were succesfuly updated\n%!"
  | "forex" -> Stocks_api.update_forex ();
    printf "Forex was succesfuly updated\n%!"
  | "splits" -> Stocks_api.update_splits ();
    printf "Splits were succesfuly updated\n%!"
  | "history" -> Stocks_api.update_history_prices ();
    printf "Historical data was succesfuly updated\n%!"
  | "targets" -> 
    let updated = Stocks_db.update_targets () in
    (match updated with
    | "OK" -> printf "Targets were succesfuly updated\n%!"
    | code -> printf "DB error updating targets: %s\n" code)
  | _ -> prerr_endline "b"

let delete_parser ticker_symbols =
  match ticker_symbols with
  | [] -> print_endline "Missing ticker symbol"
  | symbols -> 
    let rec loop symbols =
      (
        match symbols with
        | [] -> ()
        | hd :: tl ->
          String.uppercase hd |> Stocks_db.delete_stock;
          loop tl
      )
    in
    loop symbols
    
let stocks_command_parser args =
  match args with
  | [] ->
      let stock_commands =
        [
          ("update", "Args: price, stock, forex, targets");
          ("rate", "Args: fair, under, low, cheap, owned, target, saved");
          ("pull", "");
        ]
      in
      print_commands stock_commands
  | "rate" :: tl -> (
      let bad_args_exists = check_for_bad_arguments "rate" tl in
      match bad_args_exists with
      | true -> printf ""
      | false ->
          let filter = rate_command_parser tl in
          Ratings.stock_ratings filter)
  | "pull" :: tl -> (
      match tl with
      | "" :: _ -> print_endline "Ticker symbol needed"
      | ticker_symbol :: _ ->
          let up_ticker = String.uppercase ticker_symbol in
          Stocks_api.update_stock up_ticker;
          Stocks_api.update_fundamentals up_ticker;
          Stocks_api.update_price up_ticker;
          printf "%s was succesfuly pulled \n%!" (String.uppercase ticker_symbol)
      | _ -> print_endline "Ticker symbol needed")
  | "remove" :: tl -> delete_parser tl;
  | "rm" :: tl -> delete_parser tl
  | "update" :: tl -> (
      let bad_args_exists = check_for_bad_arguments "update" tl in
      match bad_args_exists with
      | true -> printf ""
      | false -> (
          match tl with
          | arg :: _ -> update_data arg
          | _ -> print_endline "Missing arguments"))
  | _ -> printf "Command not found \n"

let filter_input input = Str.split (Str.regexp " +") input

let main_command_line all_args =
  let user_input = all_args |> filter_input in
  (match user_input with
  | "exit" :: _ -> printf "\n"
  | input -> (
      match input with
      | "" :: _ -> printf "\n"
      | args -> stocks_command_parser args));
  Out_channel.flush Out_channel.stdout;
  printf ""
