package io.github.jakipatryk.sparkpersistenthomology.internal.utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/** Priority-queue-like data structure which allows to efficiently add a sorted array to it.
  *
  * It implements modulo 2 semantics, whenever cardinality of elements in the queue is 0 (modulo 2),
  * it is (lazily) eliminated.
  */
private[sparkpersistenthomology] class LazyModulo2SortedArrayPriorityQueue[T: ClassTag](implicit
  ordering: Ordering[T]
) {

  import LazyModulo2SortedArrayPriorityQueue._

  private var pq = mutable.PriorityQueue.empty[ArrayWrapper[T]](wrapperOrdering(ordering))
  private var nextElement: Option[T] = None

  def +=(sortedArray: Array[T]): Unit = {
    if (sortedArray.nonEmpty) {
      pq.enqueue(new ArrayWrapper(sortedArray, 0))
      nextElement = None
    }
  }

  def peek: Option[T] = {
    if (nextElement.isEmpty) {
      nextElement = fetchNext()
    }
    nextElement
  }

  def dequeue(): T = {
    val res = peek.getOrElse(throw new NoSuchElementException("dequeue on empty queue"))

    while (pq.nonEmpty && ordering.equiv(pq.head.array(pq.head.index), res)) {
      val w = pq.dequeue()
      while (w.index < w.array.length && ordering.equiv(w.array(w.index), res)) {
        w.index += 1
      }
      if (w.index < w.array.length) pq.enqueue(w)
    }

    nextElement = None
    res
  }

  def isEmpty: Boolean = peek.isEmpty

  def nonEmpty: Boolean = peek.isDefined

  /** Constructs a sorted Array with elements of the queue (modulo 2 adjusted). */
  def drainToArray(): Array[T] = {
    val result = ArrayBuffer.empty[T]
    while (nonEmpty) {
      result += dequeue()
    }
    result.toArray
  }

  /** An iterator for elements of the queue (modulo 2 adjusted).
    *
    * When iterating over the queue, it is safe to add (`+=`) elements to it as long as added array
    * is not greater (in `ordering`) than the currently processed element.
    */
  def iterator: Iterator[T] = new Iterator[T] {

    override def hasNext: Boolean = LazyModulo2SortedArrayPriorityQueue.this.nonEmpty

    override def next(): T = LazyModulo2SortedArrayPriorityQueue.this.dequeue()

  }

  override def clone(): LazyModulo2SortedArrayPriorityQueue[T] = {
    val newQueue = new LazyModulo2SortedArrayPriorityQueue[T]()
    this.pq.foreach(w => newQueue.pq.enqueue(w.copy()))
    newQueue.nextElement = this.nextElement
    newQueue
  }

  private def fetchNext(): Option[T] = {
    while (pq.nonEmpty) {
      val currentMax = pq.head.array(pq.head.index)

      var count        = 0
      val seenWrappers = mutable.ArrayBuffer.empty[ArrayWrapper[T]]

      while (pq.nonEmpty && ordering.equiv(pq.head.array(pq.head.index), currentMax)) {
        val w = pq.dequeue()
        seenWrappers += w

        var internalIndex = w.index
        while (
          internalIndex < w.array.length && ordering.equiv(w.array(internalIndex), currentMax)
        ) {
          count += 1
          internalIndex += 1
        }
      }

      if (count % 2 != 0) {
        pq ++= seenWrappers
        return Some(currentMax)
      } else {
        seenWrappers.foreach { w =>
          while (w.index < w.array.length && ordering.equiv(w.array(w.index), currentMax)) {
            w.index += 1
          }
          if (w.index < w.array.length) pq.enqueue(w)
        }
      }
    }
    None
  }

}

private[sparkpersistenthomology] object LazyModulo2SortedArrayPriorityQueue {

  private class ArrayWrapper[T](val array: Array[T], var index: Int) {
    def copy(): ArrayWrapper[T] = new ArrayWrapper(array, index)
  }

  private def wrapperOrdering[T](implicit ordering: Ordering[T]): Ordering[ArrayWrapper[T]] =
    new Ordering[ArrayWrapper[T]] {
      override def compare(x: ArrayWrapper[T], y: ArrayWrapper[T]): Int =
        ordering.compare(x.array(x.index), y.array(y.index))
    }

}
