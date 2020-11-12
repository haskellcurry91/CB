def groupPrefix[T](xs: List[T])(p: T => Boolean): List[List[T]] = xs match {
  case List() => List()
  case mm @ (x :: xs1) =>
    val (ys, zs) = mm span (!p(_))
//    println(ys,zs)
    (ys, zs) match {
      case  a @ (List(0,_*),List(1,_*)) =>a._1 ++ a._2.take(1) :: groupPrefix(a._2.drop(1))(p)
      case b @ (Nil,List(1,_*)) => List() ++ b._2.take(1) :: groupPrefix(b._2.drop(1))(p)
      case c @ (List(0,_*),Nil) => groupPrefix(List())(p)
      case _ => groupPrefix(List())(p)
    }
}


val a1 = List(0,0,1,1,0,0,1,0)

groupPrefix(a1)(_ == 1)

//def recursion_chain[Touch](xs: List[Touch])(p: Touch => Boolean): List[List[Touch]] = xs match {
//  case List() => List()
//  case lvalid @(x :: xs) =>
//    val (prev,next ) = lvalid.span(!p(_))
//    (prev, next) match {
//      case  alpha @ (List(Touch(_,_,"no_conv"),_*),List(Touch(_,_,"conv"),_*)) => alpha._1 ++ alpha._2.take(1) :: recursion_chain(alpha._2.drop(1))(p)
//      case beta @ (Nil,List(Touch(_,_,"conv"),_*))                             => List() ++ beta._2.take(1) :: recursion_chain(beta._2.drop(1))(p)
//      case gamma @ (List(Touch(_,_,"no_conv"),_*),Nil)                         => recursion_chain(List())(p)
//      case _                                                                   => recursion_chain(List())(p)
//    }
//}

println(List(1,2,3)(2))