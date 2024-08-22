SELECT p.FirstName, p.LastName, e.JobTitle
FROM Person.Person AS p
JOIN HumanResources.Employee AS e
    ON p.BusinessEntityID = e.BusinessEntityID
WHERE e.JobTitle = 'Design Engineer'
   OR e.JobTitle = 'Tool Designer'
   OR e.JobTitle = 'Marketing Assistant'