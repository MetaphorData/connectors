MERGE INTO TargetProducts AS Target USING SourceProducts AS Source ON Source.ProductID = Target.ProductID /* For Inserts */
WHEN NOT MATCHED THEN
INSERT
    (ProductID, ProductName, Price)
VALUES
    ('<REDACTED>', '<REDACTED>', '<REDACTED>') /* For Updates */
    WHEN MATCHED THEN
UPDATE
SET
    Target.ProductName = Source.ProductName,
    Target.Price = Source.Price