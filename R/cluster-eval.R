#' Evaluate an expression on a cluster
#'
#' @export
cluster_eval <- function(cluster, expr) {
  cluster_eval_(cluster, substitute(expr))
}

#' @export
#' @rdname cluster_eval
cluster_eval_ <- function(cluster, expr) {
  stopifnot(is.atomic(expr) || is.name(expr) || is.call(expr))
  cluster_call(cluster, eval, expr)
}
