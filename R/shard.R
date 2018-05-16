#' Partition data across a cluster.
#'
#' @param .data,data Dataset to partition
#' @param ... Variables to partition by. Will generally work best when you
#'   have many more groups than nodes. If omitted, will randomly partition
#'   rows across nodes.
#' @param groups The equivalent of \code{...} for the SE \code{partition_()}.
#' @param cluster Cluster to use.
#' @export
#' @examples
#' library(dplyr)
#' s <- partition(mtcars)
#' s %>% mutate(cyl2 = 2 * cyl)
#' s %>% filter(vs == 1)
#' s %>% summarise(n())
#' s %>% select(-cyl)
#'
#' if (require("nycflights13")) {
#' planes <- partition(flights, tailnum)
#' summarise(planes, n())
#'
#' month <- partition(flights, month)
#' month %>% group_by(day) %>% summarise(n())
#' }
partition <- function(.data, ..., cluster = get_default_cluster()) {
  groups <- rlang::quos(...)

  n <- nrow(.data)
  m <- length(cluster)

  if (length(groups) == 0) {
    part_id <- sample(floor(m * (seq_len(n) - 1) / n + 1))
    n_groups <- m

    .data$PARTITION_ID <- part_id
    quo_id <- rlang::quo(PARTITION_ID)
    .data <- dplyr::group_by(.data, !!quo_id)
    group_vars <- list(rlang::quo_expr(quo_id))
  } else {
    group_vars <- grouping_vars(!!!groups)

    .data <- dplyr::group_by(.data, !!!groups)
    group_id <- dplyr::group_indices(.data)
    n_groups <- dplyr::n_groups(.data)

    groups <- scramble_rows(dplyr::data_frame(
      id = seq_len(n_groups),
      n = tabulate(group_id, n_groups)
    ))
    groups$part_id <- floor(m * (cumsum(groups$n) - 1) / sum(groups$n) + 1)
    part_id <- groups$part_id[match(group_id, groups$id)]
  }

  idx <- split(seq_len(n), part_id)
  shards <- lapply(idx, function(i) .data[i, , drop = FALSE])

  if(length(shards) < length(cluster)) {
    cluster <- cluster[1:length(shards)]
  }

  # Init future plan
  future::plan(future::cluster, workers = cluster, persistent = TRUE)

  name <- random_table_name()
  cluster_assign_each(cluster, name, shards)

  party_df(name, cluster, group_vars)

}

grouping_vars <- function(...) {

  vars <- rlang::quos(...)

  is_name <- vapply(vars, function(x) is.name(rlang::quo_expr(x)), logical(1))

  if (any(!is_name)) {
    stop("All partition vars must already exist", call. = FALSE)
  }

  lapply(vars, rlang::quo_expr)
}

party_df <- function(name, cluster, partition = list(), groups = partition) {
  stopifnot(is.character(name), length(name) == 1)
  stopifnot(is.list(groups))

  structure(
    list(
      cluster = cluster,
      name = name,
      partitions = groups,
      groups = groups,
      deleter = shard_deleter(name, cluster)
    ),
    class = "party_df"
  )
}

shard_deleter <- function(name, cluster) {
  reg.finalizer(environment(), function(...) {
    cluster_rm(cluster, name)
  })
  environment()
}


shard_rows <- function(x) {
  call <- substitute(nrow(x), list(x = as.name(x$name)))
  nrows <- cluster_eval_(x, call)

  unlist(nrows)
}

shard_cols <- function(x) {
  call <- substitute(ncol(x), list(x = as.name(x$name)))
  cluster_eval_(x$cluster[1], call)[[1]]
}

#' @importFrom dplyr tbl_vars
#' @export
tbl_vars.party_df <- function(x) {
  call <- substitute(dplyr::tbl_vars(x), list(x = as.name(x$name)))
  cluster_eval_(x$cluster[1], call)[[1]]
}

#' @importFrom dplyr groups
#' @export
groups.party_df <- function(x) {
  call <- substitute(dplyr::groups(x), list(x = as.name(x$name)))
  cluster_eval_(x$cluster[1], call)[[1]]
}

#' @export
dim.party_df <- function(x) {
  c(sum(shard_rows(x)), shard_cols(x))
}

#' @importFrom utils head
#' @export
head.party_df <- function(x, n = 6L, ...) {
  pieces <- vector("list", length(x$cluster))
  left <- n

  # For future evaluation, evaluate head() on every worker, then only take what we need
  call <- substitute(head(x, n), list(x = as.name(x$name), n = left))
  head_all <- cluster_eval_(x$cluster, call)

  for(ii in seq_along(x$cluster)) {
    pieces[[ii]] <- head_all[[ii]]
    left <- left - nrow(head_all[[ii]])
    if (left <= 0)
      break
  }

  head_full <- dplyr::bind_rows(pieces)
  head_full[seq_len(n),]
}

#' @export
print.party_df <- function(x, ..., n = NULL, width = NULL) {
  cat("Source: party_df ", dplyr::dim_desc(x), "\n", sep = "")

  if (length(x$groups) > 0) {
    groups <- vapply(x$groups, as.character, character(1))
    cat("Groups: ", paste0(groups, collapse = ", "), "\n", sep = "")
  }

  shards <- shard_rows(x)
  cat("Shards: ", length(shards),
      " [", big_mark(min(shards)), "--", big_mark(max(shards)), " rows]\n",
      sep = "")
  cat("\n")
  print(dplyr::trunc_mat(x, n = n, width = width))

  invisible(x)
}

#' @export
as.data.frame.party_df <- function(x, row.names, optional, ...) {
  dplyr::bind_rows(cluster_get(x, x$name))
}

#' @importFrom dplyr collect
#' @method collect party_df
#' @export
collect.party_df <- function(.data, ...) {
  group_by_(as.data.frame(.data), .dots = c(.data$partitions, .data$groups))
}

# Methods passed on to shards ---------------------------------------------

#' @importFrom dplyr mutate
#' @method mutate party_df
#' @export
mutate.party_df <- function(.data, ...) {
  shard_call(.data, quote(dplyr::mutate), ...)
}

#' @importFrom dplyr filter
#' @method filter party_df
#' @export
filter.party_df <- function(.data, ...) {
  shard_call(.data, quote(dplyr::filter), ...)
}

#' @importFrom dplyr summarise
#' @method summarise party_df
#' @export
summarise.party_df <- function(.data, ...) {
  shard_call(.data, quote(dplyr::summarise), ...,
             groups = .data$groups[-length(.data$groups)])
}

#' @importFrom dplyr select
#' @method select party_df
#' @export
select.party_df <- function(.data, ...) {
  shard_call(.data, quote(dplyr::select), ...)
}

#' @importFrom dplyr group_by
#' @method group_by party_df
#' @export
group_by.party_df <- function(.data, ..., add = FALSE) {
  dots <- rlang::quos(...)
  groups <- c(.data$partitions, grouping_vars(!!!dots))

  shard_call(.data, quote(dplyr::group_by), !!!groups, groups = groups)
}

#' @importFrom dplyr do
#' @method do party_df
#' @export
do.party_df <- function(.data, ...) {
  shard_call(.data, quote(dplyr::do), ...)
}

shard_call <- function(party_df, fun, ..., groups = party_df$partition) {
  dots <- rlang::quos(...)
  call <- rlang::call2(fun, as.name(party_df$name), !!!dots)

  new_name <- random_table_name()
  cluster_assign_expr(party_df, new_name, call)
  party_df(new_name, party_df$cluster, party_df$partition, groups)
}

scramble_rows <- function(df) {
  df[sample(nrow(df)), , drop = FALSE]
}
