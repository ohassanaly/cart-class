# IMPORTS
library(haven)
library(dplyr)

ccam_df <- read_sas("~/sasdata1/cart_class_R/data/cart_class_ccam.sas7bdat") #all CCAM even
colnames(ccam_df)[colnames(ccam_df) == "BEN_NIR_PSA"] <- "NIR_ANO_17"
ucd_df <- read_sas("~/sasdata1/cart_class_R/data/cart_class_ucd.sas7bdat") #all UCD events

n_cart <- union(unique(ccam_df$NIR_ANO_17), unique(ucd_df$NIR_ANO_17))
print(length(unique(ccam_df$NIR_ANO_17)))
print(length(unique(ucd_df$NIR_ANO_17)))
print(length(n_cart))

cart_df <- bind_rows(ccam_df, ucd_df)

# Sort by EXE_SOI_DTD and remove duplicates based on NIR_ANO_17
cart_df <- cart_df %>%
  arrange(EXE_SOI_DTD) %>%      # Sort by EXE_SOI_DTD
  distinct(NIR_ANO_17, .keep_all = TRUE) # Remove duplicates based on NIR_ANO_17

## pre processing
cart_df$EXE_SOI_DTD <- as.Date(cart_df$EXE_SOI_DTD, format = "%Y-%m-%d")


# Define the cancer categories
lymphome <- c('C829', 'C831', 'C833', 'C840', 'C841', 'C842', 'C843', 'C844', 'C845', 'C846', 'C847', 'C848', 'C849',
              'C819','C838','C839','C859','C835','C830','C821','C837','C857','C851','C884','C822','C823','C820','C827',
              'C824','C811', "C852",'C810', 'C825','C880')
lal <- c('C910', 'C911', 'C920')
myelome <- c('C900', 'C901', 'C902')


# Assign cancer type based on DGN_PAL first, then check DGN_REL if needed
cart_df <- cart_df %>%
  mutate(
    cancer = case_when(
      DGN_PAL %in% lymphome ~ "lymphome",
      DGN_PAL %in% lal ~ "lal",
      DGN_PAL %in% myelome ~ "myelome",
      DGN_REL %in% lymphome ~ "lymphome",
      DGN_REL %in% lal ~ "lal",
      DGN_REL %in% myelome ~ "myelome",
      TRUE ~ NA_character_ 
      # TRUE ~ as.character(DGN_PAL)  # If no match, assign DGN_PAL itself
    )
  )

print(length(cart_df$NIR_ANO_17[cart_df$cancer %in% c("lymphome", "myelome", "lal")]))

