package main

import (
	"fmt"
	"github.com/Shopify/ivm/lang/go/client"
)

func main() {
	fmt.Println(fmt.Sprintf("client.MaxBatchSize: %v", client.MaxBatchSize))
}
