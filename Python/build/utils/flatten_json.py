def flatten(df):
	"""
	flatten:
	=====================================================================================
	Function: Flattens the input JSON schema to resolve complex data types such as StructType and ArrayType
	=====================================================================================
	Parameters:
		- df : spark dataframe containing JSON data 
	=====================================================================================
	"""
	complex_fields = dict([
		(field.name, field.dataType) 
		for field in df.schema.fields 
		if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
	])
	
	# print('complex_fields dict',complex_fields)

	qualify = list(complex_fields.keys())[0] + "_"

	while len(complex_fields) != 0:
		col_name = list(complex_fields.keys())[0]
		
		if isinstance(complex_fields[col_name], T.StructType):
			expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
						for k in [ n.name for n in  complex_fields[col_name]]
					]
			
			df = df.select("*", *expanded).drop(col_name)
	
		elif isinstance(complex_fields[col_name], T.ArrayType): 
			df = df.withColumn(col_name, F.explode_outer(col_name))
	
		complex_fields = dict([
			(field.name, field.dataType)
			for field in df.schema.fields
			if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
		])
		
		
	for df_col_name in df.columns:
		df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))

	return df