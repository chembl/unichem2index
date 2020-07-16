package main

import "regexp"

import "fmt"

func main() {

	fmt.Println("STARTING")

	inchi := `InChI=1S/C20H16I2N4O2/c21-13-6-15-19(16(22)7-13)26-18(9-23-24-26)17-8-14(10-25(17)20(15)27)28-11-12-4-2-1-3-5-12/h1-7,9,14,17H,8,10-11H2/t14-,17+/m1/s1`
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