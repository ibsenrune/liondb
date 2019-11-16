module Test

open JournalDb
open System
open System.Diagnostics
open System.IO
open System.Text

let uncurry f x y = f (x,y)
let curry f (x,y) = f x y
let private time f =
  let sw = Stopwatch()
  sw.Start()
  let res = f()
  sw.Stop()
  res, sw.ElapsedMilliseconds

let test size dbfile = 
  let data =
    seq { for i in 1..size do yield (Guid.NewGuid().ToString("N"), Guid.NewGuid().ToString("N")) }
    |> List.ofSeq
  let write = insert dbfile
  let read = get

  let writeAll () = List.iter (curry write) data
  let _, timeTaken = time writeAll
  timeTaken



