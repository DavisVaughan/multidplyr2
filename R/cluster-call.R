#' @export
cluster_call <- function(.cl, .fun, ...) {
  UseMethod("cluster_call")
}

#' @export
cluster_call.cluster <- function(.cl, .fun, ...) {

  dots  <- list(...)
  expr  <- quote(do.call(what = .fun, args = dots))
  envir <- environment()

  gp_dots <- getGlobalsAndPackages(dots, envir = envir)
  gp_fun  <- getGlobalsAndPackages(.fun, envir = envir)
  gp_expr <- getGlobalsAndPackages(expr, envir = envir)

  globals  <- c(gp_dots$globals, gp_fun$globals, gp_expr$globals)
  packages <- c(gp_dots$packages, gp_fun$packages, gp_fun$packages)

  fs <- vector("list", length(.cl))

  for(ii in seq_along(.cl)) {
    # make this resolve lazily
    fs[[ii]] <- future::futureCall(
      FUN = .fun,
      args = list(...),
      packages = packages,
      globals = globals,
      envir = envir,
      lazy = TRUE
    )
  }

  values(fs)
  #parallel::clusterCall(cl = .cl, fun = .fun, ...)
}

#' @export
cluster_call.src_cluster <- function(.cl, .fun, ...) {
  fs <- vector("list", length(.cl$cluster))
  for(ii in seq_along(.cl$cluster)) {
    fs[[ii]] <- future::futureCall(FUN = .fun, args = list(...))
  }
  values(fs)
  #parallel::clusterCall(cl = .cl$cluster, fun = .fun, ...)
}

#' @export
cluster_call.party_df <- function(.cl, .fun, ...) {
  cluster_call(.cl = .cl$cluster, .fun = .fun, ...)
}
