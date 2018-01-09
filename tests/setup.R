if ( .Platform$OS.type == 'windows' ) memory.limit( 256000 )

library(lodown)
lodown( "bsapuf" , output_dir = file.path( getwd() ) )
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
dbSendQuery( db , 
	"CREATE FUNCTION 
		div_noerror(l DOUBLE, r DOUBLE) 
	RETURNS DOUBLE 
	EXTERNAL NAME calc.div_noerror" 
)
dbGetQuery( db , 
	"SELECT 
		bene_age_cat_cd , 
		div_noerror( 
			COUNT(*) , 
			( SELECT COUNT(*) FROM bsa_partd_events_2008 ) 
		) AS share_bene_age_cat_cd
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
dbGetQuery( db , "SELECT QUANTILE( pde_drug_cost , 0.5 ) FROM bsa_partd_events_2008" )

dbGetQuery( db , 
	"SELECT 
		bene_sex_ident_cd , 
		QUANTILE( pde_drug_cost , 0.5 ) AS median_pde_drug_cost
	FROM bsa_partd_events_2008 
	GROUP BY bene_sex_ident_cd" 
)
dbGetQuery( db ,
	"SELECT
		AVG( pde_drug_cost )
	FROM bsa_partd_events_2008
	WHERE pde_drug_pat_pay_cd = 3"
)
dbGetQuery( db , 
	"SELECT 
		VAR_SAMP( pde_drug_cost ) , 
		STDDEV_SAMP( pde_drug_cost ) 
	FROM bsa_partd_events_2008" 
)

dbGetQuery( db , 
	"SELECT 
		bene_sex_ident_cd , 
		VAR_SAMP( pde_drug_cost ) AS var_pde_drug_cost ,
		STDDEV_SAMP( pde_drug_cost ) AS stddev_pde_drug_cost
	FROM bsa_partd_events_2008 
	GROUP BY bene_sex_ident_cd" 
)
dbGetQuery( db , 
	"SELECT 
		CORR( CAST( brand_name_drug AS DOUBLE ) , CAST( pde_drug_cost AS DOUBLE ) )
	FROM bsa_partd_events_2008" 
)

dbGetQuery( db , 
	"SELECT 
		bene_sex_ident_cd , 
		CORR( CAST( brand_name_drug AS DOUBLE ) , CAST( pde_drug_cost AS DOUBLE ) )
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
