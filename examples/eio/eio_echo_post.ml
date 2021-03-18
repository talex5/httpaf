module Arg = Caml.Arg

open Httpaf_eio

let request_handler (_ : Unix.sockaddr) = Httpaf_examples.Server.echo_post
let error_handler (_ : Unix.sockaddr) = Httpaf_examples.Server.error_handler

let main port =
  let listen_address = Unix.(ADDR_INET (inet_addr_loopback, port)) in
  let socket = Unix.(socket PF_INET SOCK_STREAM 0) in
  Unix.setsockopt socket Unix.SO_REUSEADDR true;
  Unix.bind socket listen_address;
  Unix.listen socket 5;
  let socket = Eunix.of_unix_file_descr socket in
  let handler = Server.create_connection_handler ~request_handler ~error_handler in
  Stdio.printf "Listening on port %i and echoing POST requests.\n" port;
  Stdio.printf "To send a POST request, try one of the following\n\n";
  Stdio.printf "  echo \"Testing echo POST\" | dune exec examples/eio/eio_post.exe\n";
  Stdio.printf "  echo \"Testing echo POST\" | dune exec examples/lwt/lwt_post.exe\n";
  Stdio.printf "  echo \"Testing echo POST\" | curl -XPOST --data @- http://localhost:%d\n\n%!" port;
  let rec loop () =
    let client_socket, client_addr = Eunix.accept socket in
    let _ = Eunix.fork (fun () -> (* XXX: handle errors! *)
        try handler client_addr client_socket
        with ex -> Logs.err (fun f -> f "Uncaught exception handling client: %a" Fmt.exn ex)
      )
    in
    loop ()
  in
  loop ()

let () =
  let port = ref 8080 in
  Arg.parse
    ["-p", Arg.Set_int port, " Listening port number (8080 by default)"]
    ignore
    "Echoes POST requests. Runs forever.";
  Eunix.run (fun () -> main !port)
