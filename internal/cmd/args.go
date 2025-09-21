package cmd

type Arg struct {
	Name  string
	Value string
}

// --name value
// -n value
// --flag
// -f
// value
func Parse(args []string) []Arg {
	res := []Arg{}

	for i := 0; i < len(args); i++ {
		s := args[i]
		if len(s) == 0 {
			continue
		}

		arg := Arg{}

		if s[0] == '-' && s[1] == '-' {
			arg.Name = s[2:]
			if i+1 < len(args) && args[i+1][0] != '-' {
				arg.Value = args[i+1]
				i++
			}

			res = append(res, arg)
			continue
		}

		if s[0] == '-' {
			arg.Name = s[1:]
			if i+1 < len(args) && args[i+1][0] != '-' {
				arg.Value = args[i+1]
				i++
			}

			res = append(res, arg)
			continue
		}

		arg.Value = s
		res = append(res, arg)
	}

	return res
}
