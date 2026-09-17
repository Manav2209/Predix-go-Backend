package dto

// SignupRequest - what the client sends for signup
type SignupRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=6"`
}

// SignupResponse - what we return after signup
type SignupResponse struct {
	ID        string `json:"id"`
	Email     string `json:"email"`
	CreatedAt string `json:"created_at"`
}

// SigninRequest - what the client sends for signin
type SigninRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

// SigninResponse - what we return after signin
type SigninResponse struct {
	Token string       `json:"token"`
	User  UserResponse `json:"user"`
}

// UserResponse - reusable user data
type UserResponse struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}
