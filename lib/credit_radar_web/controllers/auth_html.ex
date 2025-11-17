defmodule CreditRadarWeb.AuthHTML do
  use CreditRadarWeb, :html
  import CreditRadarWeb.CoreComponents

  def render("new.html", assigns) do
    ~H"""
    <div class="min-h-screen bg-slate-950 flex items-center justify-center px-4 py-12">
      <div class="w-full max-w-md rounded-3xl border border-white/10 bg-white/5 p-8 shadow-2xl shadow-black/40 backdrop-blur">
        <div class="mb-10 space-y-3 text-center">
          <div class="mx-auto flex size-14 items-center justify-center rounded-2xl bg-gradient-to-r from-[#0ADC7D] via-[#4BA5FF] to-[#6E82FA] text-white shadow-inner shadow-black/40">
            <.icon name="hero-bolt" class="size-7" />
          </div>
          <div>
            <p class="text-lg font-semibold text-white">Credit Radar</p>
            <p class="text-sm uppercase tracking-[0.4em] text-white/60">Admin Console</p>
          </div>
          <p class="text-sm text-white/70">
            Faça login para acessar os módulos administrativos e acompanhar as execuções.
          </p>
        </div>

        <.form for={@form} id="login-form" action={~p"/login"} method="post" class="space-y-6">
          <.input field={@form[:email]} type="email" label="E-mail" placeholder="usuario@email.com" required />
          <.input field={@form[:password]} type="password" label="Senha" placeholder="••••••••" required />

          <%= if assigns[:error_message] do %>
            <p class="rounded-xl border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-100">
              {assigns[:error_message]}
            </p>
          <% end %>

          <.button class="w-full justify-center bg-[#0ADC7D] text-slate-900 hover:bg-[#82FFBE] transition">
            Entrar
          </.button>
        </.form>
      </div>
    </div>
    """
  end
end
