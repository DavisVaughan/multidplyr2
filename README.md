
<!-- README.md is generated from README.Rmd. Please edit that file -->

# multidplyr2

`multidplyr2` is a revamp of `multidplyr` that uses a subset of the
`future` package as the backend. The interface is essentially the same,
but globals and packages are automatically picked up and exported to
each worker. If for some reason that fails, you can fallback to
exporting variables and packages manually.

The default is to use a local PSOCK cluster, but you can create a forked
cluster on a Unix machine and use that instead. `future` has a nice
`makeClusterPSOCK()` function that provides additional functionality for
connecting to clusters on external machines.

*There is still more work to do. I would like to update the internals
from `lazyeval` to `rlang`. I’m also still thinking about what other
benefits come from using `future`.*

## Installation

You can install the released version of multidplyr2 from
[CRAN](https://CRAN.R-project.org) with:

``` r
# No you cannot
install.packages("multidplyr2")
```

And the development version from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("DavisVaughan/multidplyr2")
```

# Example

Let’s partition `iris` by `Species`.

``` r
iris_part <- partition(iris, Species)
#> Initialising 3 core cluster.
#> Warning: group_indices_.grouped_df ignores extra arguments

iris_part
#> Source: party_df [150 x 5]
#> Groups: Species
#> Shards: 3 [50--50 rows]
#> 
#> # S3: party_df
#>    Sepal.Length Sepal.Width Petal.Length Petal.Width Species  
#>           <dbl>       <dbl>        <dbl>       <dbl> <fct>    
#>  1          6.3         3.3          6           2.5 virginica
#>  2          5.8         2.7          5.1         1.9 virginica
#>  3          7.1         3            5.9         2.1 virginica
#>  4          6.3         2.9          5.6         1.8 virginica
#>  5          6.5         3            5.8         2.2 virginica
#>  6          7.6         3            6.6         2.1 virginica
#>  7          4.9         2.5          4.5         1.7 virginica
#>  8          7.3         2.9          6.3         1.8 virginica
#>  9          6.7         2.5          5.8         1.8 virginica
#> 10          7.2         3.6          6.1         2.5 virginica
#> # ... with 140 more rows
```

We can load libraries and create variables like normal, write code that
works on our local copy of the data, then just replace the local
variable name with the partitioned variable name and the same code
works, but now in parallel.

``` r
# Library like normal. Make variables like normal
library(purrr)
x <- 1
```

Run locally to make sure things are working.

``` r
iris %>% mutate(y = map_dbl(Petal.Width, ~ .x + 2 * x))
#> # A tibble: 150 x 6
#>    Sepal.Length Sepal.Width Petal.Length Petal.Width Species     y
#>           <dbl>       <dbl>        <dbl>       <dbl> <fct>   <dbl>
#>  1          5.1         3.5          1.4         0.2 setosa    2.2
#>  2          4.9         3            1.4         0.2 setosa    2.2
#>  3          4.7         3.2          1.3         0.2 setosa    2.2
#>  4          4.6         3.1          1.5         0.2 setosa    2.2
#>  5          5           3.6          1.4         0.2 setosa    2.2
#>  6          5.4         3.9          1.7         0.4 setosa    2.4
#>  7          4.6         3.4          1.4         0.3 setosa    2.3
#>  8          5           3.4          1.5         0.2 setosa    2.2
#>  9          4.4         2.9          1.4         0.2 setosa    2.2
#> 10          4.9         3.1          1.5         0.1 setosa    2.1
#> # ... with 140 more rows
```

Switch out the local variable name for the partitioned one. Rows are
scrambled so don’t let that mess with you.

**Note that the `purrr` package and the `x` variable are automatically
exported for you by `future`\!**

``` r
iris_part %>% mutate(y = map_dbl(Petal.Width, ~ .x + 2 * x))
#> Source: party_df [150 x 6]
#> Groups: Species
#> Shards: 3 [50--50 rows]
#> 
#> # S3: party_df
#>    Sepal.Length Sepal.Width Petal.Length Petal.Width Species       y
#>           <dbl>       <dbl>        <dbl>       <dbl> <fct>     <dbl>
#>  1          6.3         3.3          6           2.5 virginica   4.5
#>  2          5.8         2.7          5.1         1.9 virginica   3.9
#>  3          7.1         3            5.9         2.1 virginica   4.1
#>  4          6.3         2.9          5.6         1.8 virginica   3.8
#>  5          6.5         3            5.8         2.2 virginica   4.2
#>  6          7.6         3            6.6         2.1 virginica   4.1
#>  7          4.9         2.5          4.5         1.7 virginica   3.7
#>  8          7.3         2.9          6.3         1.8 virginica   3.8
#>  9          6.7         2.5          5.8         1.8 virginica   3.8
#> 10          7.2         3.6          6.1         2.5 virginica   4.5
#> # ... with 140 more rows
```

# Example 2

We can still export things manually if needed.

``` r
.cl <- iris_part$cluster
cluster_assign_value(.cl, "my_remote_var", 2)

iris_part %>%
  mutate(remote_var = my_remote_var)
#> Source: party_df [150 x 6]
#> Groups: Species
#> Shards: 3 [50--50 rows]
#> 
#> # S3: party_df
#>    Sepal.Length Sepal.Width Petal.Length Petal.Width Species   remote_var
#>           <dbl>       <dbl>        <dbl>       <dbl> <fct>          <dbl>
#>  1          6.3         3.3          6           2.5 virginica          2
#>  2          5.8         2.7          5.1         1.9 virginica          2
#>  3          7.1         3            5.9         2.1 virginica          2
#>  4          6.3         2.9          5.6         1.8 virginica          2
#>  5          6.5         3            5.8         2.2 virginica          2
#>  6          7.6         3            6.6         2.1 virginica          2
#>  7          4.9         2.5          4.5         1.7 virginica          2
#>  8          7.3         2.9          6.3         1.8 virginica          2
#>  9          6.7         2.5          5.8         1.8 virginica          2
#> 10          7.2         3.6          6.1         2.5 virginica          2
#> # ... with 140 more rows
```

# Fake Example

Theoretically we could take the example from
`?future::makeClusterPSOCK()` and create all kinds of different clusters
to use here. One example is an AWS EC2 running one of the RStudio AMI’s
from Louis Aslett. The code is from the help doc above and outlines how
one could use this.

``` r
## Launching worker on Amazon AWS EC2 running one of the
## Amazon Machine Images (AMI) provided by RStudio
## (http://www.louisaslett.com/RStudio_AMI/)
public_ip <- "1.2.3.4"
ssh_private_key_file <- "~/.ssh/my-private-aws-key.pem"
cl <- makeClusterPSOCK(
  ## Public IP number of EC2 instance
  public_ip,
  ## User name (always 'ubuntu')
  user = "ubuntu",
  ## Use private SSH key registered with AWS
  rshopts = c(
    "-o", "StrictHostKeyChecking=no",
    "-o", "IdentitiesOnly=yes",
    "-i", ssh_private_key_file
  ),
  ## Set up .libPaths() for the 'ubuntu' user and
  ## install future package
  rscript_args = c(
    "-e", shQuote("local({
      p <- Sys.getenv('R_LIBS_USER')
      dir.create(p, recursive = TRUE, showWarnings = FALSE)
      .libPaths(p)
    })"),
    "-e", shQuote("install.packages('future')")
  ),
  dryrun = TRUE
)


iris_part <- partition(iris, Species, cluster = cl)
```
