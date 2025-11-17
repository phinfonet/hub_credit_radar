defmodule CreditRadar.Repo.Migrations.DropCdiProjectionReferenceFk do
  use Ecto.Migration

  @constraint "cdi_projections_reference_date_fkey"

  def up do
    drop constraint(:cdi_projections, @constraint)
  end

  def down do
    execute """
    ALTER TABLE cdi_projections
    ADD CONSTRAINT #{@constraint} FOREIGN KEY (reference_date)
    REFERENCES cdi_history(reference_date) ON DELETE CASCADE
    """
  end
end
