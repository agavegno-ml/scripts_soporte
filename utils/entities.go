package utils

const (
	BasePathCreditLine = "https://internal-api.mercadolibre.com/credits/credit_lines"
	BasePathLoans      = "https://internal-api.mercadolibre.com/credits/loans"
	Base               = 10
	BitSize            = 64
)

type CreditLine struct {
	ID         int64  `json:"id"`
	BorrowerID int64  `json:"borrower_id"`
	Status     string `json:"status"`
}

type Paging struct {
	Total  int64 `json:"total"`
	Limit  int64 `json:"limit"`
	Offset int64 `json:"offset"`
}

type PagingLoans struct {
	Paging  Paging `json:"paging"`
	Results []Loan `json:"results"`
}

type Loan struct {
	ID int64 `json:"id"`
}

type PagingCreditLines struct {
	Results []CreditLine `json:"results"`
}
