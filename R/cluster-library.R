#' @export
cluster_library <- function(cluster, packages) {
  lapply(packages, library, character.only = TRUE)
  cluster_call(.cl = cluster, .fun = lapply, FUN = library, X = packages,
               character.only = TRUE)
  invisible(cluster)
}
