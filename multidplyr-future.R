library(future)
library(parallel)

stop_if_not <- function (...)
{
  res <- list(...)
  for (ii in 1L:length(res)) {
    res_ii <- .subset2(res, ii)
    if (length(res_ii) != 1L || is.na(res_ii) || !res_ii) {
      mc <- match.call()
      call <- deparse(mc[[ii + 1]], width.cutoff = 60L)
      if (length(call) > 1L)
        call <- paste(call[1L], "....")
      stop(sprintf("%s is not TRUE", sQuote(call)), call. = FALSE,
           domain = NA)
    }
  }
  NULL
}

MultidplyrFuture <- function(expr = NULL, envir = parent.frame(), substitute = FALSE,
                     globals = TRUE, persistent = FALSE, local = !persistent,
                     workers = NULL, ...) {

  if (substitute)
    expr <- substitute(expr)
  f <- ClusterFuture(expr = expr, envir = envir, substitute = FALSE,
                     globals = globals, persistent = persistent, local = local,
                     workers = workers, ...)
  structure(f, class = c("MultidplyrFuture", class(f)))
}

multidplyr <- function (expr, envir = parent.frame(), substitute = TRUE, lazy = FALSE,
                        seed = NULL, globals = TRUE, persistent = FALSE, workers = availableCores(),
                        gc = FALSE, earlySignal = FALSE, label = NULL, ...) {
  if (substitute)
    expr <- substitute(expr)
  if (is.function(workers))
    workers <- workers()
  workers <- as.integer(workers)
  stop_if_not(length(workers) == 1, is.finite(workers), workers >= 1)
  if (workers == 1L) {
    return(sequential(expr, envir = envir, substitute = FALSE,
                      lazy = TRUE, seed = seed, globals = globals, local = TRUE,
                      label = label))
  }
  workers <- future:::ClusterRegistry("start", workers = workers)
  future <- MultidplyrFuture(expr = expr, envir = envir,
                               substitute = FALSE, lazy = lazy, seed = seed, globals = globals,
                               persistent = persistent, workers = workers, gc = gc,
                               earlySignal = earlySignal, label = label, ...)
  if (!future$lazy)
    future <- run(future)
  invisible(future)
}
class(multidplyr) <- c("multidplyr", "cluster", "multiprocess", "future", "function")
attr(multidplyr, "init") <- TRUE


clone_future <- function(future) {
  classes <- class(future)
  clone <- as.environment(as.list(future, all.names=TRUE))
  class(clone) <- classes
  clone
}

mdebug <- future:::mdebug
hpaste <- future:::hpaste

run.MultidplyrFuture <- function (future, ...)
{
  debug <- getOption("future.debug", FALSE)
  if (future$state != "created") {
    label <- future$label
    if (is.null(label))
      label <- "<none>"
    stop(FutureError(sprintf("A future ('%s') can only be launched once.",
                             label), future = future))
  }
  future:::assertOwner(future)
  workers <- future$workers
  expr <- getExpression(future)
  persistent <- future$persistent
  reg <- sprintf("workers-%s", attr(workers, "name"))

  # Unlike ClusterFuture, run this on every worker
  if (is.element("covr", loadedNamespaces())) {
    if (debug)
      mdebug("covr::package_coverage() workaround ...")
    libPath <- .libPaths()[1]
    clusterCall(workers, fun = function() .libPaths(c(libPath,
                                                 .libPaths())))
    if (debug)
      mdebug("covr::package_coverage() workaround ... DONE")
  }
  if (!persistent) {
    clusterCall(workers, fun = future:::grmall)
  }
  packages <- future:::packages.Future(future)
  if (future$earlySignal && length(packages) > 0) {
    if (debug)
      mdebug("Attaching %d packages (%s) on all nodes ...",
             length(packages), hpaste(sQuote(packages)))
    clusterCall(workers, fun = requirePackages, packages)
    if (debug)
      mdebug("Attaching %d packages (%s) on all nodes ... DONE",
             length(packages), hpaste(sQuote(packages)))
  }
  globals <- future:::globals.Future(future)
  if (length(globals) > 0) {
    if (debug) {
      total_size <- future:::asIEC(future:::objectSize(globals))
      mdebug("Exporting %d global objects (%s) to all nodes ...",
             length(globals), total_size)
    }
    for (name in names(globals)) {
      value <- globals[[name]]
      if (debug) {
        size <- future:::asIEC(future:::objectSize(value))
        mdebug("Exporting %s (%s) to all nodes ...",
               sQuote(name), size)
      }
      suppressWarnings({
        clusterCall(workers, fun = future:::gassign, name, value)
      })
      if (debug)
        mdebug("Exporting %s (%s) to all nodes ... DONE",
               sQuote(name), size)
      value <- NULL
    }
    if (debug)
      mdebug("Exporting %d global objects (%s) to all nodes ... DONE",
             length(globals), total_size)
  }
  globals <- NULL

  n_workers <- length(workers)
  node_idx_vec <- vector("integer", n_workers)
  fs <- vector("list", n_workers)
  # for(ii in seq_along(workers)) {
  #   node_idx_vec[ii] <- future:::requestNode(await = function() {
  #     future:::FutureRegistry(reg, action = "collect-first")
  #   }, workers = workers)
  # }

  sendCall <- future:::importParallel("sendCall")

  for(ii in seq_along(workers)) {

    node_idx_vec[ii] <- future:::requestNode(await = function() {
      future:::FutureRegistry(reg, action = "collect-first")
    }, workers = workers)

    future_clone <- clone_future(future)

    node_idx <- node_idx_vec[ii]
    future_clone$node <- node_idx
    cl <- workers[node_idx]
    future:::FutureRegistry(reg, action = "add", future = future_clone, earlySignal = FALSE)
    sendCall(cl[[1L]], fun = future:::geval, args = list(expr))
    future_clone$state <- "running"

    fs[[ii]] <- future_clone
  }

  # node_idx <- requestNode(await = function() {
  #   FutureRegistry(reg, action = "collect-first")
  # }, workers = workers)
  # future$node <- node_idx
  # cl <- workers[node_idx]
  #
  # FutureRegistry(reg, action = "add", future = future, earlySignal = FALSE)
  # sendCall(cl[[1L]], fun = future:::geval, args = list(expr))
  # future$state <- "running"
  if (debug)
    mdebug("%s started", class(future)[1])
  invisible(fs)
}

value.list <- function(...) {
  x <- unlist(list(...), recursive = FALSE)
  future:::values.list(x)
}

options(future.debug = TRUE)

debugonce(run.MultidplyrFuture)

# Must make plan_init() test future return NA
plan(multidplyr)
debugonce(plan)

future({1+1})

plan(multisession, persistent = TRUE)

v <- vector("list", 4L)
for(ii in 1:4) {
  v[[ii]] <- future({1})
}

values(v)
