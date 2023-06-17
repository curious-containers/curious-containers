db.createUser(
	{
		user: "ccadmin",
		pwd: "SECRET",
		roles: [
			{
				role: "readWrite",
				db: "ccagency"
			}
		]
	}
);
