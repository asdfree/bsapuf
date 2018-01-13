if ( .Platform$OS.type == 'windows' ) memory.limit( 256000 )

options("lodown.cachaca.savecache"=FALSE)

library(lodown)
this_sample_break <- Sys.getenv( "this_sample_break" )
bsapuf_cat <- get_catalog( "bsapuf" , output_dir = file.path( getwd() ) )
record_categories <- ceiling( seq( nrow( bsapuf_cat ) ) / ceiling( nrow( bsapuf_cat ) / 32 ) )
bsapuf_cat <- bsapuf_cat[ record_categories == this_sample_break , ]
lodown( "bsapuf" , bsapuf_cat )
if( any( bsapuf_cat$db_tablename == 'bsa_partd_events_2008' ) ){
library(DBI)
dbdir <- file.path( getwd() , "SQLite.db" )
db <- dbConnect( RSQLite::SQLite() , dbdir )

dbSendQuery( 
	db , 
	"ALTER TABLE bsa_partd_events_2008 ADD COLUMN brand_name_drug INTEGER" 
)

dbSendQuery( db , 
	"UPDATE bsa_partd_events_2008 
	SET brand_name_drug = 
		CASE 
			WHEN pde_drug_type_cd = 1 THEN 1 
			WHEN pde_drug_type_cd = 2 THEN 0 
			ELSE NULL 
		END" 
)
dbGetQuery( db , "SELECT COUNT(*) FROM bsa_partd_events_2008" )

dbGetQuery( db ,
	"SELECT
		bene_sex_ident_cd ,
		COUNT(*) 
	FROM bsa_partd_events_2008
	GROUP BY bene_sex_ident_cd"
)
dbGetQuery( db , "SELECT AVG( pde_drug_cost ) FROM bsa_partd_events_2008" )

dbGetQuery( db , 
	"SELECT 
		bene_sex_ident_cd , 
		AVG( pde_drug_cost ) AS mean_pde_drug_cost
	FROM bsa_partd_events_2008 
	GROUP BY bene_sex_ident_cd" 
)
dbGetQuery( db , 
	"SELECT 
		bene_age_cat_cd , 
		COUNT(*) / ( SELECT COUNT(*) FROM bsa_partd_events_2008 ) 
			AS share_bene_age_cat_cd
	FROM bsa_partd_events_2008 
	GROUP BY bene_age_cat_cd" 
)
dbGetQuery( db , "SELECT SUM( pde_drug_cost ) FROM bsa_partd_events_2008" )

dbGetQuery( db , 
	"SELECT 
		bene_sex_ident_cd , 
		SUM( pde_drug_cost ) AS sum_pde_drug_cost 
	FROM bsa_partd_events_2008 
	GROUP BY bene_sex_ident_cd" 
)
RSQLite::initExtension( db )

dbGetQuery( db , 
	"SELECT 
		LOWER_QUARTILE( pde_drug_cost ) , 
		MEDIAN( pde_drug_cost ) , 
		UPPER_QUARTILE( pde_drug_cost ) 
	FROM bsa_partd_events_2008" 
)

dbGetQuery( db , 
	"SELECT 
		bene_sex_ident_cd , 
		LOWER_QUARTILE( pde_drug_cost ) AS lower_quartile_pde_drug_cost , 
		MEDIAN( pde_drug_cost ) AS median_pde_drug_cost , 
		UPPER_QUARTILE( pde_drug_cost ) AS upper_quartile_pde_drug_cost
	FROM bsa_partd_events_2008 
	GROUP BY bene_sex_ident_cd" 
)
dbGetQuery( db ,
	"SELECT
		AVG( pde_drug_cost )
	FROM bsa_partd_events_2008
	WHERE pde_drug_pat_pay_cd = 3"
)
RSQLite::initExtension( db )

dbGetQuery( db , 
	"SELECT 
		VARIANCE( pde_drug_cost ) , 
		STDEV( pde_drug_cost ) 
	FROM bsa_partd_events_2008" 
)

dbGetQuery( db , 
	"SELECT 
		bene_sex_ident_cd , 
		VARIANCE( pde_drug_cost ) AS var_pde_drug_cost ,
		STDEV( pde_drug_cost ) AS stddev_pde_drug_cost
	FROM bsa_partd_events_2008 
	GROUP BY bene_sex_ident_cd" 
)
library(dplyr)
dplyr_db <- dplyr::src_sqlite( dbdir )
bsapuf_tbl <- tbl( dplyr_db , 'bsa_partd_events_2008' )
bsapuf_tbl %>%
	summarize( mean = mean( pde_drug_cost ) )

bsapuf_tbl %>%
	group_by( bene_sex_ident_cd ) %>%
	summarize( mean = mean( pde_drug_cost ) )
dbGetQuery( db , "SELECT COUNT(*) FROM bsa_partd_events_2008" )
}
