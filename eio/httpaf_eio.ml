(*----------------------------------------------------------------------------
    Copyright (c) 2018 Inhabited Type LLC.
    Copyright (c) 2018 Anton Bachin

    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in the
       documentation and/or other materials provided with the distribution.

    3. Neither the name of the author nor the names of his contributors
       may be used to endorse or promote products derived from this software
       without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE CONTRIBUTORS ``AS IS'' AND ANY EXPRESS
    OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED.  IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR
    ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
    OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
    HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
    STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
  ----------------------------------------------------------------------------*)

open Fibreslib

module Buffer : sig
  type t

  val create : Uring.Region.chunk -> t

  val get : t -> f:(Cstruct.t -> int) -> int
  val put : t -> f:(Cstruct.t -> int) -> int
  val commit : t -> int -> unit
  val peek : t -> Cstruct.t
  val read : t -> Eunix.FD.t -> int
end = struct
  type t =
    { chunk       : Uring.Region.chunk
    ; buffer      : Cstruct.t
    ; mutable off : int
    ; mutable len : int }

  let create chunk =
    let buffer = Cstruct.of_bigarray (Uring.Region.to_bigstring chunk) in
    { chunk; buffer; off = 0; len = 0 }

  let compress t =
    if t.len = 0
    then begin
      t.off <- 0;
      t.len <- 0;
    end else if t.off > 0
    then begin
      Cstruct.blit t.buffer t.off t.buffer 0 t.len;
      t.off <- 0;
    end

  let get t ~f =
    let n = f (Cstruct.sub t.buffer t.off t.len) in
    t.off <- t.off + n;
    t.len <- t.len - n;
    if t.len = 0
    then t.off <- 0;
    n

  let peek t =
    Cstruct.sub t.buffer t.off t.len

  let put t ~f =
    compress t;
    let n = f (Cstruct.sub t.buffer (t.off + t.len) (Cstruct.length t.buffer - t.len)) in
    t.len <- t.len + n;
    n

  let commit t n =
    t.off <- t.off + n;
    t.len <- t.len - n;
    if t.len = 0
    then t.off <- 0

  let read t fd =
    compress t;
    let n = Eunix.read_upto fd t.chunk (Uring.Region.length t.chunk - t.len) in
    t.len <- t.len + n;
    n
end

let read fd buffer =
  match Buffer.read buffer fd with
  | got -> `Ok got
  | exception End_of_file
  | exception Unix.Unix_error(Unix.ECONNRESET, _, _) -> `Eof

let cstruct_of_faraday { Faraday.buffer; off; len } = Cstruct.of_bigarray ~off ~len buffer

let write fd iovecs =
  let chunk = Eunix.alloc () in
  Fun.protect ~finally:(fun () -> Eunix.free chunk) @@ fun () ->
  let bs = (Cstruct.of_bigarray (Uring.Region.to_bigstring chunk)) in
  let chunk_size = Cstruct.length bs in
  (* Fill [bs] from [src], flushing when full. *)
  let rec aux src =
    let avail, src = Cstruct.fillv ~dst:bs ~src in
    if avail > 0 then (
      Eunix.write fd chunk avail;
      aux src
    )
  in
  aux (List.map cstruct_of_faraday iovecs)

let shutdown socket command =
  try Eunix.shutdown socket command
  with Unix.Unix_error (Unix.ENOTCONN, _, _) -> ()

module Config = Httpaf.Config

module Server = struct
  let create_connection_handler ?(config=Config.default) ~request_handler ~error_handler =
    fun client_addr socket ->
      let module Server_connection = Httpaf.Server_connection in
      let request_handler = request_handler client_addr in
      let error_handler = error_handler client_addr in
      Eunix.with_chunk @@ fun read_chunk ->
      let read_buffer = Buffer.create read_chunk in
      let read committed =
        Buffer.commit read_buffer committed;
        let more =
          match Buffer.read read_buffer socket with
          | exception End_of_file
          | exception Unix.Unix_error(Unix.ECONNRESET, _, _) -> Angstrom.Unbuffered.Complete
          | _ -> Angstrom.Unbuffered.Incomplete
        in
        let { Cstruct.buffer; off; len } = Buffer.peek read_buffer in
        buffer, off, len, more
      in
      let write io_vectors =
        match write socket io_vectors with
        | () -> `Ok (List.fold_left (fun acc f -> acc + f.Faraday.len) 0 io_vectors)
        | exception Unix.Unix_error (Unix.EPIPE, _, _) -> `Closed
      in
      let _ = Server_connection.handle ~error_handler ~read ~write request_handler in
      Eunix.FD.close socket
end


module Client = struct
  let request ?(config=Config.default) socket request ~error_handler ~response_handler =
    let module Client_connection = Httpaf.Client_connection in
    let request_body, connection =
      Client_connection.request ~config request ~error_handler ~response_handler in
    Eunix.with_chunk @@ fun read_chunk ->
    let read_buffer = Buffer.create read_chunk in
    let rec read_loop () =
      match Client_connection.next_read_operation connection with
      | `Read ->
        begin match read socket read_buffer with
          | `Eof ->
            let _ : int = Buffer.get read_buffer ~f:(fun { Cstruct.buffer = bigstring; off; len } ->
                Client_connection.read_eof connection bigstring ~off ~len) in
            read_loop ()
          | `Ok _ ->
            let _ : int = Buffer.get read_buffer ~f:(fun { Cstruct.buffer = bigstring; off; len } ->
                Client_connection.read connection bigstring ~off ~len) in
            read_loop ()
        end
      | `Close ->
        if Eunix.FD.is_open socket then shutdown socket Unix.SHUTDOWN_RECEIVE;
        raise Exit
    in
    let rec write_loop () =
      match Client_connection.next_write_operation connection with
      | `Write io_vectors ->
        let result =
          match write socket io_vectors with
          | () -> `Ok (List.fold_left (fun acc f -> acc + f.Faraday.len) 0 io_vectors)
          | exception Unix.Unix_error (Unix.EPIPE, _, _) -> `Closed
        in
        Client_connection.report_write_result connection result;
        write_loop ()
      | `Yield ->
        let pause, resume = Promise.create () in
        Client_connection.yield_writer connection (fun () -> Promise.fulfill resume ());
        Promise.await pause;
        write_loop ()
      | `Close _ ->
        if Eunix.FD.is_open socket then shutdown socket Unix.SHUTDOWN_SEND;
        raise Exit
    in
    Fibre.fork_detach
      ~on_error:(fun ex -> Logs.err (fun f -> f "Error handling client connection: %a" Fmt.exn ex))
      (fun () ->
         let read_thread =
           Fibre.fork (fun () ->
               try
                 read_loop ()
               with
               | Exit -> Logs.info (fun f -> f "Read loop done")
               | ex ->
                 Logs.warn (fun f -> f "Error reading from connection: %a" Fmt.exn ex);
                 Client_connection.report_exn connection ex
             )
         in
         let write_thread =
           Fibre.fork (fun () ->
               try
                 write_loop ()
               with
               | Exit -> Logs.info (fun f -> f "Write loop done")
               | ex ->
                 Logs.warn (fun f -> f "Error writing to connection: %a" Fmt.exn ex);
                 Client_connection.report_exn connection ex
             )
         in
         Promise.await read_thread;
         Promise.await write_thread;
         Eunix.FD.close socket;
      );
    request_body
end
