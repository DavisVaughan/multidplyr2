#' @export
create_cluster <- function(cores = NA, quiet = FALSE, ...) {
  if (is.na(cores)) {
    cores <- guess_cores()
  }

  if (!quiet) message("Initialising ", cores, " core cluster.")
  sock_cluster <- future::makeClusterPSOCK(cores, ...)
  #cluster <- parallel::makePSOCKcluster(cores)
  attr(sock_cluster, "finaliser") <- cluster_stop(sock_cluster)

  # Init plan (do this in shard so we can use custom clusters)
  # future::plan(future::cluster, workers = sock_cluster, persistent = TRUE)

  sock_cluster
}

guess_cores <- function() {
  if (in_check()) {
    return(2L)
  }

  max <- future::availableCores()
  #max <- parallel::detectCores()
  if (max == 1L) 1L else pmax(2L, max - 1L)
}


cluster_stop <- function(x) {
  reg.finalizer(environment(), function(...) {
    parallel::stopCluster(x)
  })
  environment()
}

in_check <- function() {
  paths <- strsplit(getwd(), "/", fixed = TRUE)[[1]]
  any(grepl("Rcheck$", paths))
}
