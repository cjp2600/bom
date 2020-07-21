package bom

// BeforeInsert call before saving model
type BeforeInsert interface {
	BeforeInsert() error
}

// AfterInsert call after model has been created
type AfterInsert interface {
	AfterInsert() error
}

// BeforeUpdate call before update model
type BeforeUpdate interface {
	BeforeUpdate() error
}

// AfterUpdate call after model has been updated
type AfterUpdate interface {
	AfterUpdate() error
}

// BeforeDelete call before delete model
type BeforeDelete interface {
	BeforeDelete() error
}

// AfterDelete call after model has been deleted
type AfterDelete interface {
	AfterDelete() error
}

// callToAfterDelete internal method
func callToAfterDelete(document interface{}) error {
	if event, ok := document.(AfterDelete); ok {
		if err := event.AfterDelete(); err != nil {
			return err
		}
	}
	return nil
}

// callToBeforeDelete internal method
func callToBeforeDelete(document interface{}) error {
	if event, ok := document.(BeforeDelete); ok {
		if err := event.BeforeDelete(); err != nil {
			return err
		}
	}
	return nil
}

// callToAfterUpdate internal method
func callToAfterUpdate(document interface{}) error {
	if event, ok := document.(AfterUpdate); ok {
		if err := event.AfterUpdate(); err != nil {
			return err
		}
	}
	return nil
}

// callToBeforeUpdate internal method
func callToBeforeUpdate(document interface{}) error {
	if event, ok := document.(BeforeUpdate); ok {
		if err := event.BeforeUpdate(); err != nil {
			return err
		}
	}
	return nil
}

// callToAfterInsert internal method
func callToAfterInsert(document interface{}) error {
	if event, ok := document.(AfterInsert); ok {
		if err := event.AfterInsert(); err != nil {
			return err
		}
	}
	return nil
}

// callToBeforeInsert internal method
func callToBeforeInsert(document interface{}) error {
	if event, ok := document.(BeforeInsert); ok {
		if err := event.BeforeInsert(); err != nil {
			return err
		}
	}
	return nil
}
