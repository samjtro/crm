package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Data struct {
	Leads []Lead
	Deals []Deal
	Tasks []Task
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
	Id          string
	OwnerId     string
	Subject     string
	DueDate     string
	RelatedTo   string
	RelatedToId string
	Status      string
	Priority    string
	Description string
	Tags        []string
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
	for _, x := range dealLines[1:] {
		stage := Stage{
			Name:        x[7],
			Probability: x[9],
		}
		var company Company
		for _, y := range accountLines[1:] {
			if y[0] == x[5] {
				company = Company{
					Name:          y[3],
					Website:       y[8],
					AnnualRevenue: y[12],
					SicCode:       y[13],
				}
			}
		}
		var contact Contact
		for _, z := range contactLines[1:] {
			if z[0] == x[15] {
				contact = Contact{
					Id:             z[0],
					OwnerId:        z[1],
					FirstName:      z[3],
					LastName:       z[4],
					Email:          z[9],
					PhoneNumber:    z[12],
					AltPhoneNumber: z[13],
					Tags:           strings.Split(z[38], ","),
					Company:        company,
				}
			}
		}
		deals = append(deals, Deal{
			Stage:   stage,
			Contact: contact,
		})
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
			Id:          x[0],
			OwnerId:     x[1],
			Subject:     x[3],
			DueDate:     x[4],
			RelatedTo:   x[8],
			RelatedToId: x[7],
			Status:      x[9],
			Priority:    x[10],
			Description: x[17],
			Tags:        strings.Split(x[21], ","),
		})
	}
	return tasks
}

// upload deals to s3 bucket
func (d Data) uploadDealsToS3(bucketName string) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-2"))
	check(err)
	client := s3.NewFromConfig(cfg)
	var buf bytes.Buffer
	for _, x := range d.Deals {
		gob.NewEncoder(&buf).Encode(x)
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(x.Contact.Id),
			Body:   bytes.NewReader(buf.Bytes()),
		})
		check(err)
	}
}

// upload leads to s3 bucket
func (d Data) uploadLeadsToS3(bucketName string) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-2"))
	check(err)
	client := s3.NewFromConfig(cfg)
	var buf bytes.Buffer
	for _, x := range d.Leads {
		gob.NewEncoder(&buf).Encode(x)
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(x.Contact.Id),
			Body:   bytes.NewReader(buf.Bytes()),
		})
		check(err)
	}
}

// export []Contact to csv "/export.csv"
func exportToCSV(data []Contact) {
	f, err := os.Create("export.csv")
	check(err)
	defer f.Close()
	index := []string{"Id", "OwnerId", "FirstName", "LastName", "Email", "PhoneNumber", "AltPhoneNumber", "Tags", "Company"}
	w := csv.NewWriter(f)
	w.Write(index)
	for _, x := range data {
		w.Write(x.toStringArray())
	}
	w.Flush()
}

func (c Contact) toStringArray() []string {
	var words []string
	words = append(words, c.Id, c.OwnerId, c.FirstName, c.LastName, c.Email, c.PhoneNumber, c.AltPhoneNumber)
	for _, x := range c.Tags {
		words = append(words, x)
	}
	words = append(words, c.Company.toCSV("'"))
	return words
}

func (c Company) toCSV(seperator string) string {
	return c.Name + seperator + c.Website + seperator + c.AnnualRevenue + seperator + c.SicCode + c.Physical.toCSV("\"") + c.Mailing.toCSV("\"")
}

func (a Address) toCSV(seperator string) string {
	return a.Street1 + seperator + a.Street2 + seperator + a.City + seperator + a.State + seperator + a.Zip + seperator + fmt.Sprintf("%d", a.CountryCode)
}
