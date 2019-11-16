module JournalDb

open System
open System.IO
open System.Text

let insert dbfile (key : string) (value : string) = 
  let keyBytes   = key   |> Encoding.UTF8.GetBytes
  let valueBytes = value |> Encoding.UTF8.GetBytes
  let keyLengthBytes   = BitConverter.GetBytes(keyBytes.Length)
  let valueLengthBytes = BitConverter.GetBytes(valueBytes.Length)
  let record = Array.concat([| keyBytes; keyLengthBytes; valueBytes; valueLengthBytes |])
  use stream = new FileStream(dbfile, FileMode.Append)
  stream.Write(record, 0, record.Length)

let private readPages (stream : FileStream) =
  let pageSize = 4096
  let pageStart p = p * pageSize
  let pages = stream.Length / (int64 pageSize) 
  stream.Seek(0L, SeekOrigin.End) |> ignore
  seq { 
    for page in pages .. 0L do
    let (arr : byte array) = Array.zeroCreate pageSize
    let bytesRead = stream.Read(arr, (int32 page)*pageSize, pageSize)
    yield Array.take bytesRead arr |> Array.rev
  }

let readBytesReversed (stream : FileStream) =
  let pageSize = 4096
  let pageStart p = p * pageSize
  let pages = stream.Length / (int64 pageSize) 
  let readPageBackwards (stream : FileStream) =
    let toPageStart = if stream.Position < int64 pageSize then int stream.Position else pageSize 
    let bytesToRead = toPageStart
    stream.Seek(-(int64 toPageStart), SeekOrigin.Current) |> ignore
    let (arr : byte array) = Array.zeroCreate bytesToRead
    let bytesRead = stream.Read(arr, 0, bytesToRead)
    Array.take bytesRead arr |> Array.rev
  stream.Seek(0L, SeekOrigin.End) |> ignore
  seq { 
    for _ in pages .. 0L do
    yield! readPageBackwards stream
  }

let split n xs = Seq.take n xs, Seq.skip n xs
let intFromBytes bytes = BitConverter.ToInt32(bytes, 0)
let readRecordComponent (reversedBytes : seq<byte>) : seq<byte> * seq<byte> =
  let componentLengthBytes, rest = split sizeof<int> reversedBytes 
  let componentLength = componentLengthBytes |> Array.ofSeq |> Array.rev |> intFromBytes
  let componentBytes, rest = split componentLength rest
  componentBytes, rest
let rec choose f ss =
  match f ss with
  | None -> Seq.empty
  | Some (v,rest) -> seq {
    yield v
    yield! choose f rest
  }
let rec byteEqual bs1 bs2 =
  match Seq.tryHead bs1, Seq.tryHead bs2 with
  | None, None -> true
  | (Some b1, Some b2) when b1 = b2 -> byteEqual (Seq.tail bs1) (Seq.tail bs2)
  | _ -> false

let readRecords (reversedBytes : seq<byte>) =
  let parseRecord (reversedBytes : seq<byte>) : ((seq<byte> * seq<byte>) * seq<byte>) option =
    if (Seq.isEmpty reversedBytes)
    then None
    else
      let valueBytes, rest = readRecordComponent reversedBytes
      let keyBytes, rest = readRecordComponent rest
      Some((keyBytes, valueBytes), rest)
  choose parseRecord reversedBytes

let readTest file =
  use stream = new FileStream(file, FileMode.Open)
  stream |> readBytesReversed |> List.ofSeq

let get dbfile (key : string) : string option =
  let keyBytesReversed = Encoding.UTF8.GetBytes(key) |> Array.rev
  use stream = new FileStream(dbfile, FileMode.Open)
  match stream |> readBytesReversed |> readRecords |> Seq.tryFind (fun (k,v) -> byteEqual k keyBytesReversed) with
  | None -> None
  | Some (_,v) -> Some (Encoding.UTF8.GetString(Array.ofSeq v))
  


