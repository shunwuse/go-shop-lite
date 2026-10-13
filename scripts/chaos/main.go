package main

import (
	"flag"
	"log"
	"os/exec"
	"time"
)

// chaos stops one compose service for a while, then starts it back.
// It only works on containers: processes from local `go run` are not
// managed here and are left alone. Full verification (no lost or double
// orders across the outage) lands with the bff pack.
func main() {
	target := flag.String("kill", "product", "service to stop: product, order, user, nats")
	hold := flag.Duration("for", 5*time.Second, "how long to keep it down")
	flag.Parse()

	svc, ok := map[string]string{
		"product": "product",
		"order":   "order",
		"user":    "user",
		"nats":    "nats",
	}[*target]
	if !ok {
		log.Fatalf("chaos: unknown target %q", *target)
	}

	if _, err := exec.LookPath("docker"); err != nil {
		log.Fatalf("chaos: docker not found: %v", err)
	}

	log.Printf("chaos: stopping %s for %s", svc, hold)
	if out, err := exec.Command("docker", "compose", "stop", svc).CombinedOutput(); err != nil {
		log.Fatalf("chaos: stop %s: %v\n%s", svc, err, out)
	}
	log.Printf("chaos: %s down, waiting %s", svc, hold)

	time.Sleep(*hold)

	if out, err := exec.Command("docker", "compose", "start", svc).CombinedOutput(); err != nil {
		log.Fatalf("chaos: start %s: %v\n%s", svc, err, out)
	}

	log.Printf("chaos: %s is back", svc)
}
