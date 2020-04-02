--DROP TABLE Dim_Ship
CREATE TABLE Dim_Ship (
Dim_Ship_Key INT IDENTITY(1,1) PRIMARY KEY,
LogbookIdent VARCHAR(100),
ShipName VARCHAR(100),
ShipType VARCHAR(100),
Nationality VARCHAR(100)
);

--DROP TABLE Fact_Trip
CREATE TABLE Fact_Trip (
Fact_Trip_Key INT IDENTITY(1,1) PRIMARY KEY,
Dim_Ship_Key INT FOREIGN KEY REFERENCES Dim_Ship(Dim_Ship_Key),
RecID INT,
Date DATE,
Rain BIT,
Fog BIT,
Snow BIT,
Thunder BIT
);