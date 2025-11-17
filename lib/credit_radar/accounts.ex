defmodule CreditRadar.Accounts do
  @moduledoc """
  Contexto para autenticação de administradores.

  Usa o HubRepo para acessar a tabela `admins` do banco Rails em modo read-only.
  """
  import Ecto.Query, warn: false
  alias CreditRadar.HubRepo
  alias CreditRadar.Accounts.Admin

  @doc """
  Autentica admin por email e senha.

  Usa BCrypt para validar contra o hash do Devise/Rails.

  ## Exemplos

      iex> authenticate_admin("admin@example.com", "correct_password")
      {:ok, %Admin{}}

      iex> authenticate_admin("admin@example.com", "wrong_password")
      {:error, :invalid_credentials}

      iex> authenticate_admin("nonexistent@example.com", "password")
      {:error, :invalid_credentials}
  """
  def authenticate_admin(email, password) when is_binary(email) and is_binary(password) do
    admin =
      Admin
      |> where([a], a.email == ^String.downcase(String.trim(email)))
      |> where([a], a.is_active == true)
      |> HubRepo.one()

    cond do
      # Admin encontrado e senha válida
      admin && Bcrypt.verify_pass(password, admin.encrypted_password) ->
        {:ok, admin}

      # Admin existe mas senha errada ou admin não existe
      true ->
        # Previne timing attack executando hash mesmo quando admin não existe
        Bcrypt.no_user_verify()
        {:error, :invalid_credentials}
    end
  end

  @doc """
  Busca admin por ID (para restaurar sessão).

  Levanta `Ecto.NoResultsError` se o admin não existe ou está inativo.

  ## Exemplos

      iex> get_admin!(123)
      %Admin{}

      iex> get_admin!(456)
      ** (Ecto.NoResultsError)
  """
  def get_admin!(id) do
    Admin
    |> where([a], a.id == ^id)
    |> where([a], a.is_active == true)
    |> HubRepo.one!()
  end

  @doc """
  Busca admin por ID.

  Retorna `nil` se o admin não existe ou está inativo.

  ## Exemplos

      iex> get_admin(123)
      %Admin{}

      iex> get_admin(456)
      nil
  """
  def get_admin(id) do
    Admin
    |> where([a], a.id == ^id)
    |> where([a], a.is_active == true)
    |> HubRepo.one()
  end

  @doc """
  Busca admin por email.

  Retorna `nil` se o admin não existe ou está inativo.

  ## Exemplos

      iex> get_admin_by_email("admin@example.com")
      %Admin{}

      iex> get_admin_by_email("nonexistent@example.com")
      nil
  """
  def get_admin_by_email(email) when is_binary(email) do
    Admin
    |> where([a], a.email == ^String.downcase(String.trim(email)))
    |> where([a], a.is_active == true)
    |> HubRepo.one()
  end
end
