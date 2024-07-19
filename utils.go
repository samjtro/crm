package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
)

type Task struct {
	Id                    string
	ExternalId            string
	OwnerExternalId       string
	Subject               string
	DueDate               string
	ContactName           string
	ContactNameExternalId string
	RelatedTo             string
	RelatedToExternalId   string
	Status                string
	Priority              string
	Description           string
	Tags                  []string
}

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
	Physical      Address
	Mailing       Address
}

type Contact struct {
	Id             string
	ExternalId     string
	OwnerId        string
	FirstName      string
	LastName       string
	Email          string
	PhoneNumber    string
	AltPhoneNumber string
	Status         string
	Tags           []string
	Company        Company
}

type Lead struct {
	Stage   string
	Contact Contact
}

type Deal struct {
	Stage   string `default:"deal"`
	Contact Contact
}

func check(err error) {
	if err != nil {
		log.Fatalf(err.Error())
	}
}

func importZohoContacts(path string) []Contact {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	lines, err := csv.NewReader(f).ReadAll()
	check(err)
	var contacts []Contact
	for i, x := range lines[0] {
		fmt.Println(i, x)
	}
	return contacts
}

func importZohoDeals(path string) []Deal {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	lines, err := csv.NewReader(f).ReadAll()
	check(err)
	var deals []Deal
	for i, x := range lines[0] {
		fmt.Println(i, x)
	}
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
		leads = append(leads, Lead{Stage: "lead", Contact: Contact{
			Company:        company,
			ExternalId:     x[0],
			OwnerId:        x[1],
			FirstName:      x[4],
			LastName:       x[5],
			Email:          x[6],
			PhoneNumber:    x[7],
			AltPhoneNumber: x[32],
			Status:         x[9],
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
			ExternalId:            x[0],
			OwnerExternalId:       x[1],
			Subject:               x[3],
			DueDate:               x[4],
			ContactName:           x[6],
			ContactNameExternalId: x[5],
			RelatedTo:             x[8],
			RelatedToExternalId:   x[7],
			Status:                x[9],
			Priority:              x[10],
			Description:           x[17],
			Tags:                  strings.Split(x[21], ","),
		})
	}
	return tasks
}

// create s3 bucket
func createBucket() {}

// upload csv to your s3 bucket
func uploadCsv() {}
