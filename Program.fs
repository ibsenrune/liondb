open System

type Tree<'a> =
    | Branch of 'a Tree * 'a Tree * 'a
    | Leaf

let rec map f tree =
    match tree with
    | Leaf -> Leaf
    | Branch (left, right, v) -> Branch(map f left, map f right, f v) 

type Pos = Pos of int
type PersistenceTree<'a> = (Pos * 'a) Tree

let rec insert tree value =
    match tree with
    | Leaf -> Branch(Dirty, PNone, Dirty, PNone, value)
    | Branch(_, _, v) when v = value -> PBranch(Clean, PNone, Clean, PNone, v)
    | Branch(left, _, v) when v > value -> PBranch(Dirty, insert left value, Clean, PNone, v)
    | Branch(_, right, v) when v < value -> PBranch(Clean, PNone, Dirty, insert right value, v)

let rec getDirty pt =
    match pt with
    | PNone
    | PBranch(Clean, _, Clean, _, _) -> Leaf
    | PBranch(Dirty, left, Clean, _, v) -> Branch(getDirty left, Leaf, v)
    | PBranch(Clean, _, Dirty, right, v) -> Branch(Leaf, getDirty right, v)
    | PBranch(Dirty, left, Dirty, right, v) -> Branch(getDirty left, getDirty right, v)

type SerialisationTree = (int * string) Tree


[<EntryPoint>]
let main argv =
    printfn "Hello World from F#!"
    0 // return an integer exit code
