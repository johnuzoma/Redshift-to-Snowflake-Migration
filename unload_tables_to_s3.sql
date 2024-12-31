-- unload category table
UNLOAD ('SELECT * FROM public.category')
TO 's3://redshift-data-ju/category/'
IAM_ROLE 'arn:aws:iam::116981776188:role/Redshift-S3-access'
PARQUET
PARALLEL OFF
ALLOWOVERWRITE;

-- unload date table
UNLOAD ('SELECT * FROM public.date')
TO 's3://redshift-data-ju/date/'
IAM_ROLE 'arn:aws:iam::116981776188:role/Redshift-S3-access'
PARQUET
PARALLEL OFF
ALLOWOVERWRITE;

-- unload venue table
UNLOAD ('SELECT * FROM public.venue')
TO 's3://redshift-data-ju/venue/'
IAM_ROLE 'arn:aws:iam::116981776188:role/Redshift-S3-access'
PARQUET
PARALLEL OFF
ALLOWOVERWRITE;

-- unload users table
UNLOAD ('SELECT * FROM public.users')
TO 's3://redshift-data-ju/users/'
IAM_ROLE 'arn:aws:iam::116981776188:role/Redshift-S3-access'
PARQUET
PARALLEL ON
ALLOWOVERWRITE;

-- unload sales table
UNLOAD ('SELECT * FROM public.sales')
TO 's3://redshift-data-ju/sales/'
IAM_ROLE 'arn:aws:iam::116981776188:role/Redshift-S3-access'
PARQUET
PARALLEL ON
ALLOWOVERWRITE;

-- unload listing table
UNLOAD ('SELECT * FROM public.listing')
TO 's3://redshift-data-ju/listing/'
IAM_ROLE 'arn:aws:iam::116981776188:role/Redshift-S3-access'
PARQUET
PARALLEL ON
ALLOWOVERWRITE;

-- unload event table
UNLOAD ('SELECT * FROM public.event')
TO 's3://redshift-data-ju/event/'
IAM_ROLE 'arn:aws:iam::116981776188:role/Redshift-S3-access'
PARQUET
PARALLEL OFF
ALLOWOVERWRITE;