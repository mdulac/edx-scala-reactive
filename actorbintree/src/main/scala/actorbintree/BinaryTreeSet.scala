/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo, props}
import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with Stash {

  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(r, id, elem) =>
      root ! Insert(r, id, elem)
    case Remove(r, id, elem) =>
      root ! Remove(r, id, elem)
    case Contains(r, id, elem) =>
      root ! Contains(r, id, elem)

    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context become garbageCollecting(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished =>
      root ! PoisonPill
      root = newRoot
      unstashAll()
      context become normal
    case GC =>
    case _ =>
      stash()
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(new BinaryTreeNode(elem, initiallyRemoved))
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  var parent: ActorRef = _

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(r, i, e) if e == elem =>
      removed = false
      r ! OperationFinished(i)
    case Insert(r, i, e) if e < elem =>
      subtrees.get(Left) match {
        case Some(tree) =>
          tree ! Insert(r, i, e)
        case None =>
          val tree = context.actorOf(props(e, initiallyRemoved = false))
          subtrees = subtrees.updated(Left, tree)
          tree ! Insert(r, i, e)
      }
    case Insert(r, i, e) if e > elem =>
      subtrees.get(Right) match {
        case Some(tree) =>
          tree ! Insert(r, i, e)
        case None =>
          val tree = context.actorOf(props(e, initiallyRemoved = false))
          subtrees = subtrees.updated(Right, tree)
          tree ! Insert(r, i, e)
      }

    case Remove(r, i, e) if e == elem =>
      this.removed = true
      r ! OperationFinished(i)
    case Remove(r, i, e) if e < elem =>
      if (subtrees.get(Left).isDefined) subtrees(Left) ! Remove(r, i, e) else r ! OperationFinished(i)
    case Remove(r, i, e) if e > elem =>
      if (subtrees.get(Right).isDefined) subtrees(Right) ! Remove(r, i, e) else r ! OperationFinished(i)

    case Contains(r, i, e) if e == elem =>
      r ! ContainsResult(i, result = !removed)
    case Contains(r, i, e) if e < elem =>
      if (subtrees.get(Left).isDefined) subtrees(Left) ! Contains(r, i, e) else r ! ContainsResult(i, result = false)
    case Contains(r, i, e) if e > elem =>
      if (subtrees.get(Right).isDefined) subtrees(Right) ! Contains(r, i, e) else r ! ContainsResult(i, result = false)

    case CopyTo(node) =>
      parent = sender()
      if (!removed) node ! Insert(node, -1, elem)
      (subtrees.get(Left), subtrees.get(Right)) match {
        case (Some(l), Some(r)) =>
          l ! CopyTo(node)
          r ! CopyTo(node)
          context become copying(2, parent)
        case (Some(l), None) =>
          l ! CopyTo(node)
          context become copying(1, parent)
        case (None, Some(r)) =>
          r ! CopyTo(node)
          context become copying(1, parent)
        case (None, None) =>
          parent ! CopyFinished
          self ! PoisonPill
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Int, up: ActorRef): Receive = {
    case CopyFinished if expected < 2 =>
      up ! CopyFinished
      self ! PoisonPill
    case CopyFinished =>
      context become copying(expected - 1, up)
  }


}
