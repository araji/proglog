CONFIG_PATH=${HOME}/.proglog

.PHONY: init
init:
	@mkdir -p ${CONFIG_PATH}
	@cp config.json ${CONFIG_PATH}

.PHONY: gencert
gencert:
	cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca
	
	cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=test/ca-config.json -profile=server test/server-csr.json | cfssljson -bare server
	cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=test/ca-config.json -profile=client -cn="root"   test/client-csr.json | cfssljson -bare root-client
	cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=test/ca-config.json -profile=client -cn="nobody" test/client-csr.json | cfssljson -bare nobody-client
	
	mv *.pem *.csr ${CONFIG_PATH}


.PHONY: test
test: 
	cp test/model.conf $(CONFIG_PATH)/model.conf
	cp test/policy.csv $(CONFIG_PATH)/policy.csv
	go test -v ./...

.PHONY: compile
compile:
	buf generate