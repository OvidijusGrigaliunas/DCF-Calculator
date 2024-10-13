open Base
open Stdio

let current_dir =
  let dir_with_file_name = Sys.get_argv () in
  let rec get_dir_without_name splited_dir =
    match splited_dir with
    | [] -> ""
    | [ _ ] -> ""
    | hd :: tl -> hd ^ "/" ^ get_dir_without_name tl
  in
  let splited_dir = String.split dir_with_file_name.(0) ~on:'/' in
  get_dir_without_name splited_dir

let api_key =
  let file = current_dir ^ "api_key.txt" |> In_channel.read_lines in
  match file with
  | [] -> ""
  | hd :: _ ->
      let url = "apikey=" ^ hd in
      url
