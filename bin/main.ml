open Base

let () =
  (* let a = Secrets.current_dir ^ "balance.json" |> In_channel.read_all in *)
  (* let b = Secrets.current_dir ^ "cashflow.json" |> In_channel.read_all in *)
  let args = Sys.get_argv () |> List.of_array in
  match args with
  | [] -> Command_parser.main_command_line ""
  | _ :: tl -> Command_parser.main_command_line (String.concat ~sep:" " tl)

(* let real_price = ( *)
(* match pull_data with *)
(* | true -> Lwt_main.run (Stocks_api.get_current_price tick_symbol) *)
(* | false -> 0.0) *)
(* in *)
