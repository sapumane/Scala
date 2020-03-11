package ca.mcit.dataenrichment

//**  Disclaimer: Not my code, all credit to the Author

trait Join[L, R, Q] {
  def join(a: List[L], b: List[R]): List[Q]
}

case class JoinOutput(left: Any, right: Any)

class GenericNestedLoopJoin[L, R](val joinCond: (L, R) => Boolean) extends Join[L, R, JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = for {
    i <- a
    j <- b
    if joinCond(i, j)
  } yield JoinOutput(i, j)
}

