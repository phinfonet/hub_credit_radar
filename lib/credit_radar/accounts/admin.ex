defmodule CreditRadar.Accounts.Admin do
  @moduledoc """
  Schema para a tabela `admins` do Hub Backend (Rails).

  Esta tabela está no banco de dados do Rails e é acessada em modo read-only.
  Não tente criar, atualizar ou deletar registros através deste schema.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :id, autogenerate: true}
  @foreign_key_type :id

  # Tabela criada e gerenciada pelo Rails
  schema "admins" do
    field :email, :string
    field :encrypted_password, :string
    field :name, :string
    field :is_active, :boolean, default: true

    # Campos opcionais do Devise (caso necessário no futuro)
    field :reset_password_token, :string
    field :reset_password_sent_at, :utc_datetime
    field :remember_created_at, :utc_datetime

    # IMPORTANTE: Rails usa created_at/updated_at
    # NÃO usar timestamps() que gera inserted_at/updated_at
    field :created_at, :utc_datetime
    field :updated_at, :utc_datetime
  end

  @doc """
  Changeset apenas para validações em memória.
  Não deve ser usado para inserir/atualizar no banco.
  """
  def changeset(admin, attrs) do
    admin
    |> cast(attrs, [:email, :name])
    |> validate_required([:email, :name])
    |> validate_format(:email, ~r/@/)
  end
end
