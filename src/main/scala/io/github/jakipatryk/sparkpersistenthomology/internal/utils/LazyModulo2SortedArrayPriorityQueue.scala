package io.github.jakipatryk.sparkpersistenthomology.internal.utils

/** Priority-queue-like data structure which allows to efficiently add a sorted array to it.
  *
  * It implements modulo 2 semantics, whenever cardinality of elements in the queue is 0 (modulo 2),
  * it is (lazily) eliminated.
  */
private[sparkpersistenthomology] class LazyModulo2SortedArrayPriorityQueue[T](implicit
  ordering: Ordering[T]
) {

  def +=(sortedArray: Array[T]): Unit = ???

  /** Constructs a sorted Array with elements of the queue (modulo 2 adjusted). */
  def drainToArray(): Array[T] = ???

  /** An iterator for elements of the queue (modulo 2 adjusted).
    *
    * When iterating over the queue, it is safe to add (`+=`) elements to it as long as added array
    * is not greater (in `ordering`) than the currently processed element.
    */
  def iterator: Iterator[T] = ???

}
