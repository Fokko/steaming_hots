library(tidyverse)

# You run this command with:
# Rscript plotr.R <input_file> <output_path>
# Example: 
# Rscript plotr.R ~/Desktop/input.csv ~/Desktop/demo.png

args = commandArgs(trailingOnly=TRUE)

input_file <- args[1]
output_path <- args[2]

df <- read_csv(input_file, col_names = c('hero', 'games', 'win', 'ratio')) %>% 
  mutate(hero = as.factor(hero))

p <- ggplot() + 
  geom_line(data = df, aes(games, ratio, colour = hero)) + 
  ggtitle("plot over time")

ggsave(output_path, p)