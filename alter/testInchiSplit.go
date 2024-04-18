package main

import "regexp"

import "fmt"

func main() {

	fmt.Println("STARTING")

	inchi := `InChI=1S/C20H25N3O3/c1-25-18-7-3-16(4-8-18)21-15-20(24)23-13-11-22(12-14-23)17-5-9-19(26-2)10-6-17/h3-10,21H,11-15H2,1-2H3`
	//inchi = `InChI=1S/C13H18O2/c1-9(2)8-11-4-6-12(7-5-11)10(3)13(14)15/h4-7,9-10H,8H2,1-3H3,(H,14,15)`
	fmt.Printf("INCHI: %s\n", inchi)
	splitInchi(inchi)
}

func splitInchi(inchi string) {

	connectionsReg := `(?:/c(?P<connections>[^/]*))?`
	hAtomsReg := `(?:/h(?P<hAtoms>[^/]*))?`
	chargeReg := `(?:/q(?P<charge>[^/]*))?`
	protonsReg := `(?:/p(?P<protons>[^/]*))?`
	stereoDbondReg := `(?:/b(?P<stereoDbond>[^/]*))?`
	stereoSP3Reg := `(?:/t(?P<stereoSP3>[^/]*))?`
	stereoSP3invertedReg := `(?:/m(?P<stereoSP3inverted>[^/]*))?`
	stereoTypeReg := `(?:/s(?P<stereoType>\d))?`
	isotopicAtoms := `(?:/i(?P<isotopicAtoms>[^/]*))?`
	isotopicExchangeableH := `(?:/h(?P<isotopicExchangeableH>[^/]*))?`

	inchiReg := `^InChI=(?P<version>[^/]*)/(?P<formula>[^/]*)` + connectionsReg + hAtomsReg + chargeReg + protonsReg + stereoDbondReg + stereoSP3Reg + stereoSP3invertedReg + stereoTypeReg + isotopicAtoms + isotopicExchangeableH

	r := regexp.MustCompile(inchiReg)

	res := r.FindStringSubmatch(inchi)
	if res == nil {
		fmt.Println("That doesn't looks like an InChI bruh")
		return
	}
	fmt.Println(res)
	resMap := make(map[string]string)
	names := r.SubexpNames()
	fmt.Println("---GROUPS---")
	for i, name := range r.SubexpNames() {
		if i != 0 {
			resMap[name] = res[i]
			fmt.Println(names[i], res[i])
		}
	}

	fmt.Println("MAP", resMap)
}
