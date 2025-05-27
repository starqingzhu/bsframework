package sqldb

import (
	"fmt"
)

type Decorator struct {
	decorator string
	params    []interface{}
}

func (decorator *Decorator) Where(cond string, params ...interface{}) *Decorator {
	decorator.decorator += " WHERE " + cond + " "
	decorator.params = append(decorator.params, params...)

	return decorator
}

func (decorator *Decorator) GroupBy(column string) *Decorator {
	decorator.decorator += " GROUP BY " + column + " "

	return decorator
}

func (decorator *Decorator) OrderBy(column string, asc bool) *Decorator {
	decorator.decorator += " ORDER BY " + column + " "
	if asc {
		decorator.decorator += "ASC "
	} else {
		decorator.decorator += "DESC "
	}

	return decorator
}

func (decorator *Decorator) Limit(limit1 int, limit2 int) *Decorator {
	decorator.decorator += " LIMIT " + fmt.Sprintf("%d", limit1)
	if limit2 > 0 {
		decorator.decorator += ", " + fmt.Sprintf("%d", limit2)
	}

	return decorator
}
