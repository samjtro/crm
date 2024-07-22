package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
)

type Address struct {
	Street1     string
	Street2     string
	City        string
	State       string
	Zip         string
	CountryCode int
}

type Company struct {
	Name          string
	Website       string
	AnnualRevenue string
	SicCode       string
	Physical      Address
	Mailing       Address
}

type Contact struct {
	Id             string
	OwnerId        string
	FirstName      string
	LastName       string
	Email          string
	PhoneNumber    string
	AltPhoneNumber string
	Tags           []string
	Company        Company
}

type Lead struct {
	Status  string
	Contact Contact
}

type Deal struct {
	Stage   Stage
	Contact Contact
}

type User struct {
	Id    string
	Name  string
	Email string
}

type Task struct {
	Id            string
	OwnerId       string
	Subject       string
	DueDate       string
	ContactName   string
	ContactNameId string
	RelatedTo     string
	RelatedToId   string
	Status        string
	Priority      string
	Description   string
	Tags          []string
}

type Stage struct {
	Name        string
	Probability string
}

func check(err error) {
	if err != nil {
		log.Fatalf(err.Error())
	}
}

func importZohoDeals(pathToDealsCSV, pathToAccountsCSV, pathToContactsCSV string) []Deal {
	dealFile, err := os.Open(pathToDealsCSV)
	check(err)
	defer dealFile.Close()
	accountFile, err := os.Open(pathToAccountsCSV)
	check(err)
	defer accountFile.Close()
	contactFile, err := os.Open(pathToContactsCSV)
	check(err)
	defer contactFile.Close()
	dealLines, err := csv.NewReader(dealFile).ReadAll()
	check(err)
	accountLines, err := csv.NewReader(accountFile).ReadAll()
	check(err)
	contactLines, err := csv.NewReader(contactFile).ReadAll()
	check(err)
	var deals []Deal
	for _, x1 := range dealLines[1:] {
		stage := Stage{
			Name:        x1[7],
			Probability: x1[9],
		}
		var company Company
		for _, x2 := range accountLines[1:] {
			if x2[0] == x1[5] {
				company = Company{
					Name:          x2[3],
					Website:       x2[8],
					AnnualRevenue: x2[12],
					SicCode:       x2[13],
				}
			}
		}
		var contact Contact
		for _, x3 := range contactLines[1:] {
			if x3[0] == x1[15] {
				contact = Contact{
					Id:             x3[0],
					OwnerId:        x3[1],
					FirstName:      x3[3],
					LastName:       x3[4],
					Email:          x3[9],
					PhoneNumber:    x3[12],
					AltPhoneNumber: x3[13],
					Tags:           strings.Split(x3[38], ","),
					Company:        company,
				}
			}
		}
		deals = append(deals, Deal{
			Stage:   stage,
			Contact: contact,
		})
	}
	fmt.Println(deals[0])
	return deals
}

// import CSV from Zoho "Leads" module
func importZohoLeads(path string) []Lead {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	lines, err := csv.NewReader(f).ReadAll()
	check(err)
	var leads []Lead
	for _, x := range lines[1:] {
		company := Company{
			Name:          x[3],
			Website:       x[8],
			AnnualRevenue: x[10],
			Physical: Address{
				Street1: x[12],
				City:    x[13],
				State:   x[14],
				Zip:     x[15],
			},
			Mailing: Address{
				Street1: x[41],
				City:    x[42],
				State:   x[38],
				Zip:     x[37],
			},
		}
		leads = append(leads, Lead{Status: x[9], Contact: Contact{
			Company:        company,
			Id:             x[0],
			OwnerId:        x[1],
			FirstName:      x[4],
			LastName:       x[5],
			Email:          x[6],
			PhoneNumber:    x[7],
			AltPhoneNumber: x[32],
			Tags:           strings.Split(x[19], ","),
		}})
	}
	return leads
}

func importZohoTasks(path string) []Task {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	lines, err := csv.NewReader(f).ReadAll()
	check(err)
	var tasks []Task
	for _, x := range lines[1:] {
		tasks = append(tasks, Task{
			Id:            x[0],
			OwnerId:       x[1],
			Subject:       x[3],
			DueDate:       x[4],
			ContactName:   x[6],
			ContactNameId: x[5],
			RelatedTo:     x[8],
			RelatedToId:   x[7],
			Status:        x[9],
			Priority:      x[10],
			Description:   x[17],
			Tags:          strings.Split(x[21], ","),
		})
	}
	return tasks
}

// create s3 bucket
func createBucket() {}

// upload csv to your s3 bucket
func uploadCsv() {}
