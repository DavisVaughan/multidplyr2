### ----------------------------------------------------------------------------
### Iris example

iris_part <- partition(iris, Species)

iris_part

# Library like normal. Make variables like normal
library(purrr)
x <- 1

iris %>% mutate(y = map_dbl(Petal.Width, ~ .x + 2 * x))

# black magic auto exports globals and packages needed
iris_part2 <- iris_part %>% mutate(y = map_dbl(Petal.Width, ~.x + 2 * x))

iris_part3 <- iris_part2 %>% summarise(s = sum(Sepal.Length))

### ----------------------------------------------------------------------------
#### NYC Flights

flights <- nycflights13::flights

flights_part <- flights %>% partition(carrier)

flights_part2 <- summarise(flights_part, dep_delay = mean(dep_delay, na.rm = TRUE))

# Longer example
common_dest <- flights %>%
  count(dest) %>%
  filter(n >= 365) %>%
  semi_join(flights, .) %>%
  mutate(yday = lubridate::yday(ISOdate(year, month, day)))

by_dest <- common_dest %>% partition(dest)

library(mgcv)

system.time({
  models <- by_dest %>%
    do(mod = gam(dep_delay ~ s(yday) + s(dep_time), data = .))
})


system.time({
  models <- common_dest %>%
    group_by(dest) %>%
    do(mod = gam(dep_delay ~ s(yday) + s(dep_time), data = .))
})

### ----------------------------------------------------------------------------
### Custom cluster

data(FANG, package = "tidyquant")

.cl <- create_cluster(4)

FANG_part <- partition(FANG, symbol, cluster = .cl)

FANG_part %>%
  mutate(adjusted_ret = adjusted / lag(adjusted) - 1)

FANG_part %>%
  mutate(idx = rep(1:2, 1008/2)) %>%
  group_by(idx) %>%
  summarise(mean_adj = mean(volume))

### ----------------------------------------------------------------------------
### Some tibbletime uses

library(tibbletime)

FANG_part %>%
  mutate(date2 = collapse_index(date, "yearly")) %>%
  group_by(date2) %>%
  summarise(yearly_mean_adj = mean(adjusted))

roll_mean <- rollify(mean, window = 5)

FANG_part %>%
  mutate(rolled_mean = roll_mean(adjusted))

lm_roll <- rollify(~lm(.x ~ .y), window = 5, unlist = FALSE)

system.time({
  FANG_part %>%
    mutate(rolled_lm = lm_roll(adjusted, volume))
})

system.time({
  FANG %>%
    mutate(rolled_lm = lm_roll(adjusted, volume))
})

### ----------------------------------------------------------------------------
### Running random code over there

# Let's place a random variable over on each node
cluster_assign_value(.cl, "x_var", 2)

# Its not over here, but we can use it there
FANG %>% mutate(x = x_var)

FANG_part %>% mutate(x = x_var)


### ----------------------------------------------------------------------------
### Lets just break everything
#
# library(furrr)
# library(purrr)
#
# FANG_part %>%
#   mutate(x = pmap_dbl(FANG_part[,c("open", "high")], .f = ~ .x + .y))



