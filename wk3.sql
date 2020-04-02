--DROP TABLE Dim_Ship
CREATE TABLE Dim_Ship (
Dim_Ship_Key INT IDENTITY(1,1) PRIMARY KEY,
LogbookIdent VARCHAR(100),
ShipName VARCHAR(100),
ShipType VARCHAR(100),
Nationality VARCHAR(100)
);

CREATE TABLE Fact_Trip (
LogbookIdent INT FOREIGN KEY REFERENCES Dim_Ship(Dim_Ship_Key),
RecID INT
Rain BIT,
Fog BIT,
Snow BIT,
Thunder BIT,
Date DATE,
);