package sql

import "testing"

func expectSQL(t *testing.T, gen *SQLGenerator, expected string) {
	actual := WriteString(gen)
	if actual != expected {
		t.Errorf("EXPECTED:\n----------------\n%s\nACTUAL:\n--------------\n%s\n", expected, actual)
	}
}

func TestSingleTable(t *testing.T) {
	db := MakeDatabase("StarDB")
	franchise := MakeTable("DimFranchise", "id")

	db.AddTable(franchise)

	gen := new(SQLGenerator)
	gen.DB = &db
	gen.AddSelect(Select{
		Column: "FranchiseName",
		Table:  franchise.Name,
	})

	expected := "select\n" +
		"    DimFranchise.FranchiseName\n" +
		"from DimFranchise"
	expectSQL(t, gen, expected)
}

func TestOneDimension(t *testing.T) {
	db := MakeDatabase("StarDB")
	franchise := MakeTable("DimFranchise", "id")

	sales := MakeTable("FactSales", "id")
	sales.AddFKey(ForeignKey{
		Key:    "FranchiseId",
		Target: franchise.Name,
	})

	db.AddTable(franchise)
	db.AddTable(sales)

	gen := new(SQLGenerator)
	gen.DB = &db
	gen.FromName = sales.Name
	gen.AddSelect(Select{
		Column: "FranchiseName",
		Table:  franchise.Name,
	})

	expected := "select\n" +
		"    DimFranchise.FranchiseName\n" +
		"from FactSales\n" +
		"join DimFranchise on DimFranchise.id = FactSales.FranchiseId"
	expectSQL(t, gen, expected)
}

func TestTwoDimension(t *testing.T) {
	db := MakeDatabase("StarDB")
	franchise := MakeTable("DimFranchise", "id")
	store := MakeTable("DimStore", "id")

	sales := MakeTable("FactSales", "id")
	sales.AddFKey(ForeignKey{
		Key:    "FranchiseId",
		Target: franchise.Name,
	})
	sales.AddFKey((ForeignKey{
		Key:    "StoreId",
		Target: store.Name,
	}))

	db.AddTable(sales)
	db.AddTable(franchise)
	db.AddTable(store)

	gen := new(SQLGenerator)
	gen.DB = &db
	gen.FromName = sales.Name
	gen.AddSelect(Select{
		Column: "FranchiseName",
		Table:  franchise.Name,
	})
	gen.AddSelect(Select{
		Column: "StoreName",
		Table:  store.Name,
	})

	expected := "select\n" +
		"    DimFranchise.FranchiseName,\n" +
		"    DimStore.StoreName\n" +
		"from FactSales\n" +
		"join DimFranchise on DimFranchise.id = FactSales.FranchiseId\n" +
		"join DimStore on DimStore.id = FactSales.StoreId"
	expectSQL(t, gen, expected)
}

func TestAggregate(t *testing.T) {
	db := MakeDatabase("StarDB")
	franchise := MakeTable("DimFranchise", "id")
	store := MakeTable("DimStore", "id")

	sales := MakeTable("FactSales", "id")
	sales.AddFKey(ForeignKey{
		Key:    "FranchiseId",
		Target: franchise.Name,
	})
	sales.AddFKey((ForeignKey{
		Key:    "StoreId",
		Target: store.Name,
	}))

	db.AddTable(sales)
	db.AddTable(franchise)
	db.AddTable(store)

	gen := new(SQLGenerator)
	gen.DB = &db
	gen.FromName = sales.Name
	gen.AddSelect(Select{
		Column: "FranchiseName",
		Table:  franchise.Name,
	})
	gen.AddSelect(Select{
		Column: "StoreName",
		Table:  store.Name,
	})
	gen.AddSelect(Select{
		Aggregate: "sum",
		Column:    "Quantity",
		Table:     sales.Name,
		Alias:     "Products Sold",
	})

	expected := "select\n" +
		"    DimFranchise.FranchiseName,\n" +
		"    DimStore.StoreName,\n" +
		"    sum(FactSales.Quantity) as \"Products Sold\"\n" +
		"from FactSales\n" +
		"join DimFranchise on DimFranchise.id = FactSales.FranchiseId\n" +
		"join DimStore on DimStore.id = FactSales.StoreId\n" +
		"group by\n" +
		"    DimFranchise.FranchiseName,\n" +
		"    DimStore.StoreName"
	expectSQL(t, gen, expected)
}
